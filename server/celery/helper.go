package celery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"fxdayu.com/dyupdater/server/utils"
	"github.com/deckarep/golang-set"
	"github.com/gomodule/redigo/redis"
	"github.com/spf13/viper"
)

var log = utils.AppLogger

type CeleryTaskResponse struct {
	CeleryID string `json:"task-id"`
	State    string `json:"state"`
	Result   string `json:"result"`
	Error    string
}

type CeleryTaskInput struct {
	ID      string
	Payload interface{}
}

type celeryExec struct {
	Type    string `json:"exc_type"`
	Message string `json:"exc_message"`
}

type CeleryTaskResult struct {
	ID       string
	Response CeleryTaskResponse
}

type asyncApplyResult struct {
	CeleryID string `json:"task-id"`
}

type celeryHelperConfig struct {
	Host         string   `mapstructure:"host"`
	Backend      string   `mapstructure:"backend"`
	Broker       string   `mapstructure:"broker"`
	Queues       []string `mapstructure:"queues"`
	CleanBackend bool     `mapstructure:"clean-backend"`
	CleanBroker  bool     `mapstructure:"clean-broker"`
}

type CeleryHelper struct {
	config          *celeryHelperConfig
	running         bool
	store           taskMapStore
	backend         *CustomRedisCeleryBackend
	broker          *AMQPCeleryBroker
	clients         []*FlowerWsClient
	chEvent         chan interface{}
	chEventLimit    map[string]chan bool
	chEventLimitCap map[string]int
	chQueryAll      chan bool
	out             map[string]chan CeleryTaskResult
	term            chan bool
}

// CeleryService Celery是python的分布式任务队列模块
// CeleryService的配置对应于配置文件中的celery一项
// celery配置项是一个字典，每一个字典项对应一个celery实例的配置，key对应
// celery实例的名字，value对应celery实例的信息。
// 每一个celery实例可以包含的配置项有：
//  host #字符串，celery flower的host地址，默认localhost:5555
//  backend #字符串，redis的url地址，默认redis://localhost
//  broker #字符串，broker的url地址，默认amqp://guest:guest@localhost
//  queues #字符串数组，对应broker的所有queue名字
//  clean-broker #bool,启动时是否清理broker，默认True
//  clean-backend #bool，启动时是否要清理backend，默认True
//配置示例：
/*
  celery:
    default:
      host: 127.0.0.1:5555
      backend: redis://127.0.0.1/0
      broker: amqp://guest:guest@127.0.0.1
      queues:
      - factor
      - stores
*/
type CeleryService struct {
	config  *viper.Viper
	helpers map[string]*CeleryHelper
}

func (cs *CeleryService) Get(name string) (*CeleryHelper, bool) {
	helper, ok := cs.helpers[name]
	return helper, ok
}

func (cs *CeleryService) MustGet(name string) *CeleryHelper {
	helper, ok := cs.helpers[name]
	if !ok {
		panic(fmt.Errorf("no celery names %s with proper config", name))
	}
	return helper
}

var celeryServiceInstance *CeleryService

func GetCeleryService() *CeleryService {
	if celeryServiceInstance == nil {
		g := viper.GetViper()
		var config *viper.Viper
		if g.IsSet("celery") {
			config = g.Sub("celery")
		} else {
			config = viper.New()
		}
		celeryServiceInstance = &CeleryService{
			config:  config,
			helpers: make(map[string]*CeleryHelper),
		}
		for key := range config.AllSettings() {
			helper := NewCeleryHelper(config.Sub(key))
			celeryServiceInstance.helpers[key] = helper
			helper.Run()
		}
	}
	return celeryServiceInstance
}

func (helper *CeleryHelper) Publish(taskID string, task string, routerKey string, payload []byte) (string, error) {
	host := helper.config.Host
	url := url.URL{Scheme: "http", Host: host, Path: "/api/task/async-apply/" + task}
	buffer := bytes.NewBuffer(payload)
	req, err := http.NewRequest("POST", url.String(), buffer)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		respData := new(asyncApplyResult)
		err = json.Unmarshal(body, respData)
		if err != nil {
			return "", err
		}
		if respData.CeleryID == "" {
			return "", errors.New("Empty task id")
		}
		log.Debugf("(Task %s) commit celery task: %s", taskID, respData.CeleryID)
		info := &taskInfo{taskID: taskID, routerKey: routerKey}
		helper.store.Set(respData.CeleryID, info)
		return respData.CeleryID, nil
	}
	body, _ := ioutil.ReadAll(resp.Body)
	return "", fmt.Errorf("HttpError: %d,%s", resp.StatusCode, body)
}

func (helper *CeleryHelper) Cleanup() {
	if helper.config.CleanBackend {
		helper.CleanBackend()
	}
	if helper.config.CleanBroker {
		helper.CleanBroker()
	}
}

func (helper *CeleryHelper) CleanBackend() {
	log.Info("Clean up unrelated and zombies celery task.")
	taskIDs, err := helper.backend.GetTaskIDs()
	if err != nil {
		log.Errorf("Clean up failed due to: %s", err.Error())
		return
	}
	storedIDs := helper.store.Iter()
	storedSet := mapset.NewSet()
	for _, v := range storedIDs {
		storedSet.Add(v)
	}
	countRevoke := 0
	countDelete := 0
	for _, taskID := range taskIDs {
		if !storedSet.Contains(taskID) {
			r, err := helper.backend.GetResult(taskID)
			if err != nil {
				log.Error(err.Error())
				continue
			}
			err = helper.backend.DeleteResult(taskID)
			if err != nil {
				log.Error(err.Error())
				continue
			}
			countDelete++
			if r.Status == "SUCCESS" || r.Status == "FAILURE" || r.Status == "ABORTED" {
				continue
			}
			log.Debugf("Revoke unrelated celery task: %s.", taskID)
			helper.revokeTask(taskID)
			countRevoke++
		}
	}
	log.Infof("Clean up finished, revoke %d task, delete %d task.", countRevoke, countDelete)
}

func (helper *CeleryHelper) CleanBroker() {
	log.Info("Purge all amqp queues.")
	for _, name := range helper.config.Queues {
		for i := 0; i < 3; i++ {
			count, err := helper.broker.QueuePurge(name, false)
			if err != nil {
				log.Error(err.Error())
			} else {
				log.Infof("Purge %d task in queue %s", count, name)
				break
			}
		}
	}
}

func (helper *CeleryHelper) Run() {
	var err error
	helper.running = true
	helper.store = newInMemoryTaskMapStore()
	helper.backend = NewCustomRedisCeleryBackend(helper.config.Backend)
	helper.broker, err = NewAMQPCeleryBrokerWithOptions(
		helper.config.Broker, "default", "celery", 4, false,
	)
	if err != nil {
		panic(err)
	}
	log.Infof("Whether auto cleanup enabled, Broker: %t, Backend: %t.", helper.config.CleanBroker, helper.config.CleanBackend)
	helper.Cleanup()
	//only clean up broker when start up
	helper.config.CleanBroker = false
	go func() {
		for helper.running {
			time.Sleep(3600 * time.Second)
			helper.Cleanup()
		}
	}()
	helper.clients = make([]*FlowerWsClient, 2)
	helper.chEventLimitCap = make(map[string]int)
	helper.chEventLimitCap["factor.cal"] = runtime.NumCPU() // TODO config it rather than hard code
	helper.chEventLimit = make(map[string]chan bool)
	helper.chEvent = make(chan interface{}, 100000)
	helper.chQueryAll = make(chan bool)
	helper.out = make(map[string]chan CeleryTaskResult)
	helper.term = make(chan bool)
	log.Infof("Connect to flower host: %s\n", helper.config.Host)
	go func() {
		for helper.running {
			helper.queryAll()
			select {
			case <-helper.term:
				return
			case <-time.After(600 * time.Second):
				continue
			}
		}
	}()
	go func() {
		for range helper.chQueryAll {
			helper.doQueryAll()
		}
	}()
	// listen to the celery result to be set.
	// NOTE deprecate
	// go helper.listen()
	// go helper.openWs()
	helper.subscribe()
}

// NOTE deprecate
// func (helper *CeleryHelper) abortTask(taskID string) {
// 	host := helper.config.Host
// 	url := url.URL{Scheme: "http", Host: host, Path: "/api/task/abort/" + task}
// 	req, _ := http.NewRequest("POST", url.String(), bytes.NewBuffer([]byte{}))
// 	req.Header.Set("Content-Type", "application/json")
// 	client := &http.Client{}
// 	client.Do(req)
// }

func (helper *CeleryHelper) abortTask(taskID string) {
	helper.backend.AbortTask(taskID)
}

func (helper *CeleryHelper) deleteTask(taskID string) {
	helper.backend.DeleteResult(taskID)
}

func (helper *CeleryHelper) revokeTask(taskID string) {
	host := helper.config.Host
	url := url.URL{Scheme: "http", Host: host, Path: fmt.Sprintf("/api/task/revoke/%s?terminate=true", taskID)}
	req, _ := http.NewRequest("POST", url.String(), bytes.NewBuffer([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	client.Do(req)
}

func (helper *CeleryHelper) putResult(result *CeleryTaskResponse) {
	info, err := helper.store.Get(result.CeleryID)
	if err != nil {
		log.Error(err.Error())
		return
	}
	ch := helper.GetOutput(info.routerKey)
	ch <- CeleryTaskResult{ID: info.taskID, Response: *result}
	helper.limitCountDown(result.CeleryID)
	helper.store.Remove(result.CeleryID)
	helper.deleteTask(result.CeleryID)
	log.Debugf("(Task %s) celery task %s: %s and DELETED", info.taskID, result.CeleryID, result.State)
}

func (helper *CeleryHelper) fetchTask(task string) (*CeleryTaskResponse, error) {
	res, err := helper.backend.GetResult(task)
	if err != nil {
		return nil, err
	}
	respData := new(CeleryTaskResponse)
	respData.CeleryID = res.ID
	respData.State = res.Status
	if respData.State == "FAILURE" {
		if res.Traceback != nil {
			tmp, ok := res.Traceback.(string)
			if ok {
				respData.Error = tmp
			} else {
				log.Errorf("unvalid celery task traceback data: %s", res.Traceback)
				respData.Error = "unvalid celery task traceback data"
			}
		}
		var exec *celeryExec
		if res.Result != nil {
			exec = new(celeryExec)
			b, _ := json.Marshal(res.Result)
			err := json.Unmarshal(b, exec)
			if err != nil {
				log.Error(err.Error())
				exec = nil
			}
		}
		if respData.Error != "" && exec != nil {
			respData.Error = fmt.Sprintf("%s %s : %s", respData.Error, exec.Type, exec.Message)
		} else if respData.Error == "" && exec != nil {
			respData.Error = fmt.Sprintf("%s : %s", exec.Type, exec.Message)
		} else {
			respData.Error = "no valid celery task failure infomation"
		}
	} else if res.Result != nil {
		// TODO fix Result interface
		tmp, ok := res.Result.(string)
		if ok {
			respData.Result = tmp
		} else {
			log.Errorf("unvalid celery result: %s", res.Result)
			respData.Error = "unvalid celery result, must be string"
			respData.State = "FAILURE"
		}
	}
	return respData, nil
}

func (helper *CeleryHelper) queryAll() {
	select {
	case helper.chQueryAll <- true:
	default:
	}
}

func (helper *CeleryHelper) doQueryAll() {
	log.Info("Query status for all celery task.")
	for _, tid := range helper.store.Iter() {
		if !helper.running {
			break
		}
		helper.limitCountUp(tid)
		go func(tid string) {
			r, err := helper.fetchTask(tid)
			if err == nil {
				helper.putResult(r)
			} else {
				helper.limitCountDown(tid)
			}
		}(tid)
		time.Sleep(20 * time.Millisecond)
	}
}

func (helper *CeleryHelper) GetOutput(routerkey string) chan CeleryTaskResult {
	ch, ok := helper.out[routerkey]
	if !ok {
		ch = make(chan CeleryTaskResult)
		helper.out[routerkey] = ch
	}
	return ch
}

func (helper *CeleryHelper) getChEventLimit(routerKey string) chan bool {
	chEventLimt, ok := helper.chEventLimit[routerKey]
	if !ok {
		chEventLimitCap, ok := helper.chEventLimitCap[routerKey]
		if !ok {
			chEventLimitCap = 100000 // unlimit
		}
		chEventLimt = make(chan bool, chEventLimitCap)
		helper.chEventLimit[routerKey] = chEventLimt
	}
	return chEventLimt
}

func (helper *CeleryHelper) limitCountUp(celeryID string) {
	info, err := helper.store.Get(celeryID)
	if err != nil {
		log.Error(err.Error())
		return
	}
	chEventLimt := helper.getChEventLimit(info.routerKey)
	chEventLimt <- true
}

func (helper *CeleryHelper) limitCountDown(celeryID string) {
	info, err := helper.store.Get(celeryID)
	if err != nil {
		log.Error(err.Error())
		return
	}
	chEventLimit := helper.getChEventLimit(info.routerKey)
	select {
	case <-chEventLimit:
	default:
	}
}

func (helper *CeleryHelper) subscribe() {
	for {
		err := helper.backend.Subscribe(helper.chEvent)
		if err == nil {
			break
		}
		log.Errorf("Unable to subscribe the redis backend due to: %s!!!", err.Error())
		time.Sleep(10 * time.Second)
	}
	go func() {
		cache := make(map[string]int)
		for recv := range helper.chEvent {
			switch v := recv.(type) {
			case string:
				log.Debugf("Receive celery Event: %s", v)
				if !helper.store.IsSet(v) {
					count, ok := cache[v]
					if !ok {
						cache[v] = count
					}
					if count >= 3 {
						log.Debugf("Skip unrelated celery Event: %s", v)
						helper.deleteTask(v)
						delete(cache, v)
					} else {
						cache[v] = count + 1
						wait := time.Duration(cache[v]) * time.Second
						go func(taskID string, wait time.Duration) {
							time.Sleep(wait)
							helper.chEvent <- taskID
						}(v, wait)
					}
				} else {
					helper.limitCountUp(v)
					go func(tid string) {
						log.Debugf("Query celery task: %s", tid)
						for r, err := helper.fetchTask(tid); ; {
							if err == nil {
								helper.putResult(r)
								break
							} else {
								log.Errorf("%s", err)
							}
							time.Sleep(100 * time.Millisecond)
						}
					}(v)
				}
			case redis.Subscription:
				helper.queryAll()
			}
		}
	}()
}

// //NOTE deprecate
// func (helper *CeleryHelper) listen() {
// 	for e := range helper.event {
// 		log.Debugf("Receive celery Event: %s", e.UUID)
// 		if !helper.store.IsSet(e.UUID) {
// 			log.Debugf("Skip unrelated celery Event: %s", e.UUID)
// 			continue
// 		}
// 		helper.eventLimit <- true
// 		go func(id string) {
// 			log.Debugf("Query celery task: %s", id)
// 			for r, err := helper.fetchTask(id); ; {
// 				if err == nil {
// 					helper.putResult(r)
// 					break
// 				}
// 				time.Sleep(100 * time.Millisecond)
// 			}
// 			<-helper.eventLimit
// 		}(e.UUID)
// 	}
// }

// //NOTE deprecate
// func (helper *CeleryHelper) openWs() {
// 	host := helper.config.Host
// 	successURL := url.URL{Scheme: "ws", Host: host, Path: "/api/task/events/task-succeeded/"}
// 	helper.clients = append(helper.clients, OpenWs(helper.event, successURL.String()))
// 	failedURL := url.URL{Scheme: "ws", Host: host, Path: "/api/task/events/task-failed/"}
// 	helper.clients = append(helper.clients, OpenWs(helper.event, failedURL.String()))
// }

func (helper *CeleryHelper) GetQueue(task string, routerKey string) *CeleryQueue {
	q := &CeleryQueue{}
	q.helper = helper
	q.task = task
	q.routerKey = routerKey
	q.Output = helper.GetOutput(routerKey)
	return q
}

func (helper *CeleryHelper) Close() {
	helper.running = false
	for _, client := range helper.clients {
		client.Close()
	}
	close(helper.chEvent)
	for _, v := range helper.chEventLimit {
		close(v)
	}
	close(helper.term)
	for _, v := range helper.out {
		close(v)
	}
}

func NewCeleryHelper(config *viper.Viper) (helper *CeleryHelper) {
	celeryHelperConfig := &celeryHelperConfig{
		Host:         utils.GetEnv("FLOWER_URL", "127.0.0.1:5555"),
		Broker:       utils.GetEnv("RABBITMQ_URL", "amqp://guest:guest@localhost"),
		Backend:      utils.GetEnv("REDIS_URL", "redis://localhost"),
		Queues:       []string{"celery"},
		CleanBackend: true,
		CleanBroker:  true,
	}
	config.Unmarshal(celeryHelperConfig)
	helper = &CeleryHelper{
		config: celeryHelperConfig,
	}
	return
}

type CeleryQueue struct {
	helper    *CeleryHelper
	routerKey string
	task      string
	Output    chan CeleryTaskResult
}

func (queue *CeleryQueue) Publish(taskID string, payload []byte) (string, error) {
	return queue.helper.Publish(taskID, queue.task, queue.routerKey, payload)
}
