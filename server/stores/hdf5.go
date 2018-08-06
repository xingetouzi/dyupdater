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
	TaskFetch         string `mapstructrue:"task.fetch.name"`
	TaskCheckTimeout  int    `mapstructrue:"task.check.timeout"`
	TaskUpdateTimeout int    `mapstructrue:"task.update.timeout"`
	TaskFetchTimeout  int    `mapstructrue:"task.fetch.timeout"`
}

// HDF5Store 是将因子数据存为hdf5文件的因子存储
// 由于golang没有很好的操作hdf5文件的库，这部分的任务通过celery发送给python完成
// hdf5文件存放的目录为: ~/.fxdayu/data/dyfactors, windows上为：C:\Users\%username%\.fxdayu\data\dyfactors
// HDF5Store 的配置项有：
//   celery: celery实例名，任务会被发送到该celery实例，需要在celery配置中有对应的项。
type HDF5Store struct {
	BaseFactorStore
	common.BaseComponent
	helper           *celery.CeleryHelper
	config           *hdf5StoreConfig
	queueCheck       *celery.CeleryQueue
	queueUpdate      *celery.CeleryQueue
	queueFetch       *celery.CeleryQueue
	taskChan         map[string]chan celeryResult
	taskChanLock     sync.RWMutex
	taskCheckPrefix  string
	taskUpdatePrefix string
	taskFetchPrefix  string
	taskCheckCount   int
	taskUpdateCount  int
	taskFetchCount   int
}

type celeryResult struct {
	Data  interface{}
	Error error
}

func (s *HDF5Store) Init(config *viper.Viper) {
	s.BaseComponent.Init(config)
	s.config = &hdf5StoreConfig{
		Celery:            "default",
		TaskCheck:         "stores.hdf5_check",
		TaskUpdate:        "stores.hdf5_update",
		TaskFetch:         "stores.hdf5_fetch",
		TaskCheckTimeout:  120,
		TaskUpdateTimeout: 600,
		TaskFetchTimeout:  180,
	}
	config.Unmarshal(s.config)
	service := celery.GetCeleryService()
	s.helper = service.MustGet(s.config.Celery)
	s.taskCheckPrefix = "stores-hdf5-check"
	s.taskUpdatePrefix = "stores-hdf5-update"
	s.taskFetchPrefix = "stores-hdf5-fetch"
	s.queueCheck = s.helper.GetQueue(s.config.TaskCheck, s.taskCheckPrefix)
	s.queueUpdate = s.helper.GetQueue(s.config.TaskUpdate, s.taskUpdatePrefix)
	s.queueFetch = s.helper.GetQueue(s.config.TaskFetch, s.taskFetchPrefix)
	s.taskChan = make(map[string]chan celeryResult)
	s.taskChanLock = sync.RWMutex{}
	go s.handleCheckResult()
	go s.handleUpdateResult()
	go s.handleFetchResult()
}

func (s *HDF5Store) setTaskChan(id string, ch chan celeryResult) {
	s.taskChanLock.Lock()
	defer s.taskChanLock.Unlock()
	s.taskChan[id] = ch
}

func (s *HDF5Store) getTaskChan(id string) chan celeryResult {
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
	for result := range s.queueCheck.Output {
		rep := result.Response
		var ret celeryResult
		if rep.Error != "" {
			ret = celeryResult{
				Data:  nil,
				Error: errors.New(rep.Error),
			}
		} else {
			data := make([]int, 0)
			err := json.Unmarshal([]byte(rep.Result), &data)
			if err != nil {
				log.Error(err.Error())
				ret = celeryResult{
					Data:  nil,
					Error: errors.New(rep.Error),
				}
			}
			ret = celeryResult{
				Data:  data,
				Error: nil,
			}
		}
		ch := s.getTaskChan(result.ID)
		if ch != nil {
			ch <- ret
		} else {
			log.Warning("Unrelated celery task %s of stores.hdf5-check", result.ID)
		}
	}
}

func (s *HDF5Store) handleUpdateResult() {
	for result := range s.queueUpdate.Output {
		rep := result.Response
		var ret celeryResult
		if rep.Error != "" {
			ret = celeryResult{
				Data:  0,
				Error: errors.New(rep.Error),
			}
		} else {
			var data int
			err := json.Unmarshal([]byte(rep.Result), &data)
			if err != nil {
				log.Error(err.Error())
				ret = celeryResult{
					Data:  0,
					Error: errors.New(rep.Error),
				}
			}
			ret = celeryResult{
				Data:  data,
				Error: nil,
			}
		}
		ch := s.getTaskChan(result.ID)
		if ch != nil {
			ch <- ret
		} else {
			log.Warning("Unrelated celery task %s of stores.hdf5-update", result.ID)
		}
	}
}

func (s *HDF5Store) handleFetchResult() {
	for result := range s.queueFetch.Output {
		rep := result.Response
		var ret celeryResult
		if rep.Error != "" {
			ret = celeryResult{
				Data:  nil,
				Error: errors.New(rep.Error),
			}
		} else {
			data := models.FactorValue{}
			err := utils.ParseFactorValue(rep.Result, &data)
			if err != nil {
				log.Error(err.Error())
				ret = celeryResult{
					Data:  nil,
					Error: err,
				}
			}
			ret = celeryResult{
				Data:  data,
				Error: err,
			}
		}
		ch := s.getTaskChan(result.ID)
		if ch != nil {
			ch <- ret
		} else {
			log.Warning("Unrelated celery task %s of stores.hdf5-fetch", result.ID)
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
	ch := make(chan celeryResult)
	s.setTaskChan(taskID, ch)
	defer s.removeTaskChan(taskID)
	select {
	case r := <-ch:
		if r.Error != nil {
			return nil, r.Error
		}
		result, ok := r.Data.([]int)
		if !ok {
			return nil, fmt.Errorf("Invalid check Result: %v", r.Data)
		}
		return result, r.Error
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
	ch := make(chan celeryResult)
	s.setTaskChan(taskID, ch)
	defer s.removeTaskChan(taskID)
	select {
	case r := <-ch:
		if r.Error != nil {
			return 0, r.Error
		}
		result, ok := r.Data.(int)
		if !ok {
			return 0, fmt.Errorf("Invalid check Result: %v", r.Data)
		}
		return result, r.Error
	case <-time.After(time.Duration(s.config.TaskUpdateTimeout) * time.Second):
		return 0, fmt.Errorf("hdf5 update task timeout after %d seconds", s.config.TaskUpdateTimeout)
	}
}

func (s *HDF5Store) Fetch(factor models.Factor, dateRange models.DateRange) (models.FactorValue, error) {
	data := map[string][]interface{}{
		"args": []interface{}{factor.ID, dateRange.Start, dateRange.End},
	}
	jsonData, err := json.Marshal(data)
	s.taskFetchCount++
	taskID := fmt.Sprintf("%s-%d-%d", s.taskFetchPrefix, time.Now().Unix(), s.taskFetchCount)
	_, err = s.queueFetch.Publish(taskID, jsonData)
	if err != nil {
		return models.FactorValue{}, err
	}
	ch := make(chan celeryResult)
	s.setTaskChan(taskID, ch)
	defer s.removeTaskChan(taskID)
	select {
	case r := <-ch:
		if r.Error != nil {
			return models.FactorValue{}, r.Error
		}
		result, ok := r.Data.(models.FactorValue)
		if !ok {
			return models.FactorValue{}, fmt.Errorf("Invalid fetch Result: %s", r)
		}
		return result, r.Error
	case <-time.After(time.Duration(s.config.TaskFetchTimeout) * time.Second):
		return models.FactorValue{}, fmt.Errorf("hdf5 fetch task timeout after %d seconds", s.config.TaskCheckTimeout)
	}
}

func (s *HDF5Store) Close() {
	for _, v := range s.taskChan {
		close(v)
	}
	close(s.queueCheck.Output)
	close(s.queueUpdate.Output)
	close(s.queueFetch.Output)
}
