package stores

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"fxdayu.com/dyupdater/server/celery"
	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/spf13/viper"
)

type hdf5StoreConfig struct {
	Celery            string `mapstrucetrue:"celery"`
	TaskCheck         string `mapstructrue:"task.check.name"`
	TaskUpdate        string `mapstructrue:"task.update.name"`
	TaskCheckTimeout  int    `mapstructrue:"task.check.timeout"`
	TaskUpdateTimeout int    `mapstructrue:"task.update.timeout"`
}

// HDF5Store 是将因子数据存为hdf5文件的因子存储
// 由于golang没有很好的操作hdf5文件的库，这部分的任务通过celery发送给python完成
// hdf5文件存放的目录为: ~/.fxdayu/data/dyfactors, windows上为：C:\Users\%username%\.fxdayu\data\dyfactors
// HDF5Store 的配置项有：
//   celery: celery实例名，任务会被发送到该celery实例，需要在celery配置中有对应的项。
type HDF5Store struct {
	common.BaseComponent
	helper           *celery.CeleryHelper
	config           *hdf5StoreConfig
	queueCheck       *celery.CeleryQueue
	queueUpdate      *celery.CeleryQueue
	taskChan         map[string]chan interface{}
	taskChanLock     sync.RWMutex
	taskCheckPrefix  string
	taskUpdatePrefix string
	taskCheckCount   int
	taskUpdateCount  int
}

type checkResult struct {
	Index []int
	Error error
}

type updateResult struct {
	Count int
	Error error
}

func (s *HDF5Store) Init(config *viper.Viper) {
	s.BaseComponent.Init(config)
	s.config = &hdf5StoreConfig{
		Celery:            "default",
		TaskCheck:         "stores.hdf5_check",
		TaskUpdate:        "stores.hdf5_update",
		TaskCheckTimeout:  180,
		TaskUpdateTimeout: 600,
	}
	config.Unmarshal(s.config)
	service := celery.GetCeleryService()
	s.helper = service.MustGet(s.config.Celery)
	s.taskCheckPrefix = "stores-hdf5-check"
	s.taskUpdatePrefix = "stores-hdf5-update"
	s.queueCheck = s.helper.GetQueue(s.config.TaskCheck, s.taskCheckPrefix)
	s.queueUpdate = s.helper.GetQueue(s.config.TaskUpdate, s.taskUpdatePrefix)
	s.taskChan = make(map[string]chan interface{})
	s.taskChanLock = sync.RWMutex{}
	go s.handleCheckResult()
	go s.handleUpdateResult()
}

func (s *HDF5Store) setTaskChan(id string, ch chan interface{}) {
	s.taskChanLock.Lock()
	defer s.taskChanLock.Unlock()
	s.taskChan[id] = ch
}

func (s *HDF5Store) getTaskChan(id string) chan interface{} {
	s.taskChanLock.RLock()
	defer s.taskChanLock.RUnlock()
	ch, _ := s.taskChan[id]
	return ch
}

func (s *HDF5Store) removeTaskChan(id string) {
	s.taskChanLock.Lock()
	defer s.taskChanLock.Unlock()
	ch, ok := s.taskChan[id]
	if ok {
		close(ch)
		delete(s.taskChan, id)
	}
}

func (s *HDF5Store) handleCheckResult() {
	for celeryResult := range s.queueCheck.Output {
		rep := celeryResult.Response
		var ret checkResult
		if rep.Error != "" {
			ret = checkResult{
				Index: nil,
				Error: errors.New(rep.Error),
			}
		} else {
			data := make([]int, 0)
			err := json.Unmarshal([]byte(rep.Result), &data)
			if err != nil {
				log.Error(err.Error())
				ret = checkResult{
					Index: nil,
					Error: errors.New(rep.Error),
				}
			}
			ret = checkResult{
				Index: data,
				Error: nil,
			}
		}
		ch := s.getTaskChan(celeryResult.ID)
		if ch != nil {
			ch <- ret
		} else {
			log.Warning("Unrelated celery task %s of stores.hdf5-check", celeryResult.ID)
		}
	}
}

func (s *HDF5Store) handleUpdateResult() {
	for celeryResult := range s.queueUpdate.Output {
		rep := celeryResult.Response
		var ret updateResult
		if rep.Error != "" {
			ret = updateResult{
				Count: 0,
				Error: errors.New(rep.Error),
			}
		} else {
			var data int
			err := json.Unmarshal([]byte(rep.Result), &data)
			if err != nil {
				log.Error(err.Error())
				ret = updateResult{
					Count: 0,
					Error: errors.New(rep.Error),
				}
			}
			ret = updateResult{
				Count: data,
				Error: nil,
			}
		}
		ch := s.getTaskChan(celeryResult.ID)
		if ch != nil {
			ch <- ret
		} else {
			log.Warning("Unrelated celery task %s of stores.hdf5-update", celeryResult.ID)
		}
	}
}

func (s *HDF5Store) Check(factor models.Factor, index []int) ([]int, error) {
	data := map[string][]interface{}{
		"args": []interface{}{factor.ID, index},
	}
	jsonData, err := json.Marshal(data)
	s.taskCheckCount++
	taskID := fmt.Sprintf("%s-%d-%d", s.taskCheckPrefix, time.Now().Unix(), s.taskCheckCount)
	_, err = s.queueCheck.Publish(taskID, jsonData)
	if err != nil {
		return nil, err
	}
	ch := make(chan interface{})
	s.setTaskChan(taskID, ch)
	defer s.removeTaskChan(taskID)
	select {
	case r := <-ch:
		result, ok := r.(checkResult)
		if !ok {
			return nil, fmt.Errorf("Invalid check Result: %s", r)
		}
		return result.Index, result.Error
	case <-time.After(time.Duration(s.config.TaskCheckTimeout) * time.Second):
		return nil, fmt.Errorf("hdf5 check task timeout after %d seconds", s.config.TaskCheckTimeout)
	}
}

func (s *HDF5Store) Update(factor models.Factor, factorValue models.FactorValue, replace bool) (int, error) {
	values := make(map[string][]float64)
	for k, v := range factorValue.Values {
		values[k] = v
	}
	dts := make([]float64, len(factorValue.Datetime))
	for i, v := range factorValue.Datetime {
		dts[i] = float64(v)
	}
	values["trade_date"] = dts
	tmp, err := utils.PackMsgpackSnappy(values)
	if err != nil {
		return 0, err
	}
	factorValueString := base64.StdEncoding.EncodeToString(tmp)
	data := map[string][]interface{}{
		"args": []interface{}{factor.ID, factorValueString},
	}
	jsonData, err := json.Marshal(data)
	s.taskUpdateCount++
	taskID := fmt.Sprintf("%s-%d-%d", s.taskUpdatePrefix, time.Now().Unix(), s.taskUpdateCount)
	_, err = s.queueUpdate.Publish(taskID, jsonData)
	if err != nil {
		return 0, err
	}
	ch := make(chan interface{})
	s.setTaskChan(taskID, ch)
	defer s.removeTaskChan(taskID)
	select {
	case r := <-ch:
		result, ok := r.(updateResult)
		if !ok {
			return 0, fmt.Errorf("Invalid check Result: %s", r)
		}
		return result.Count, result.Error
	case <-time.After(time.Duration(s.config.TaskUpdateTimeout) * time.Second):
		return 0, fmt.Errorf("hdf5 update task timeout after %d seconds", s.config.TaskUpdateTimeout)
	}
}

func (s *HDF5Store) Close() {
	for _, v := range s.taskChan {
		close(v)
	}
	close(s.queueCheck.Output)
	close(s.queueUpdate.Output)
}
