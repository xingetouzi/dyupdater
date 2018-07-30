package schedulers

import (
	"container/list"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
	"github.com/Jeffail/tunny"
	"github.com/panjf2000/ants"
	"github.com/spf13/viper"
)

var log = utils.AppLogger

type TaskStore interface {
	GetNextTaskId() string
	Set(id string, task *task.TaskFuture) error
	Get(id string) (*task.TaskFuture, error)
	All() ([]*task.TaskFuture, error)
	Remove(id string) error
}

type TaskScheduler interface {
	common.Configable
	SetTaskStore(TaskStore)
	Publish(parent *task.TaskFuture, input task.TaskInput) *task.TaskFuture
	SetInLimit(t int, limit int)
	SetOutLimit(t int, limit int)
	GetTasks() ([]*task.TaskFuture, error)
	AppendHandler(t int, handler func(*task.TaskFuture) error)
	AppendSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult) error)
	AppendPostSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult))
	AppendFailureHandler(t int, handler func(task.TaskFuture, error))
	AppendAbortedHandler(t int, handler func(task.TaskFuture))
	PrependHandler(t int, handler func(*task.TaskFuture) error)
	PrependSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult) error)
	PrependPostSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult))
	PrependFailureHandler(t int, handler func(task.TaskFuture, error))
	PrependAbortedHandler(t int, handler func(task.TaskFuture))
	GetOutputChan() chan task.TaskResult
	Wait(chan bool)
}

type BaseScheduler struct {
	taskStore           TaskStore
	handlers            map[int][]func(*task.TaskFuture) error
	successHandlers     map[int][]func(task.TaskFuture, task.TaskResult) error
	failedHandlers      map[int][]func(task.TaskFuture, error)
	postSuccessHandlers map[int][]func(task.TaskFuture, task.TaskResult)
	abortedHandlers     map[int][]func(task.TaskFuture)
	in                  chan string
	out                 chan task.TaskResult
	subIn               map[int]chan string
	subOut              map[int]chan task.TaskResult
	// NOTE: deprecated
	// inLimit         map[int]chan bool
	// outLimit        map[int]chan bool
	inPool    map[int]*tunny.Pool
	outPool   map[int]*tunny.Pool
	count     int
	waitChans *list.List
	waitLock  *sync.Mutex
}

func (scheduler *BaseScheduler) Init(config *viper.Viper) {
	scheduler.handlers = make(map[int][]func(*task.TaskFuture) error)
	scheduler.successHandlers = make(map[int][]func(task.TaskFuture, task.TaskResult) error)
	scheduler.postSuccessHandlers = make(map[int][]func(task.TaskFuture, task.TaskResult))
	scheduler.failedHandlers = make(map[int][]func(task.TaskFuture, error))
	scheduler.abortedHandlers = make(map[int][]func(task.TaskFuture))

	// NOTE: deprecated
	// scheduler.inLimit = make(map[int]chan bool)
	// scheduler.outLimit = make(map[int]chan bool)
	scheduler.in = make(chan string, 1000000)
	scheduler.out = make(chan task.TaskResult, 1000000)
	scheduler.subIn = make(map[int]chan string)
	scheduler.subOut = make(map[int]chan task.TaskResult)
	scheduler.inPool = make(map[int]*tunny.Pool)
	scheduler.outPool = make(map[int]*tunny.Pool)
	scheduler.waitLock = &sync.Mutex{}
	scheduler.waitChans = list.New()
}

func (scheduler *BaseScheduler) GetOutputChan() chan task.TaskResult {
	return scheduler.out
}

func (scheduler *BaseScheduler) newInPool(limit int) (*tunny.Pool, error) {
	pool := tunny.NewFunc(limit, func(v interface{}) interface{} {
		scheduler.handle(v.(*task.TaskFuture))
		return nil
	})
	return pool, nil
}

func (scheduler *BaseScheduler) newOutPool(limit int) (*tunny.Pool, error) {
	pool := tunny.NewFunc(limit, func(v interface{}) interface{} {
		scheduler.onSuccess(v.(task.TaskResult))
		return nil
	})
	return pool, nil
}

func (scheduler *BaseScheduler) SetInLimit(t int, limit int) {
	// NOTE: deprecated
	// scheduler.inLimit[t] = make(chan bool, limit)
	pool, err := scheduler.newInPool(limit)
	if err != nil {
		panic(err)
	}
	scheduler.inPool[t] = pool
}

func (scheduler *BaseScheduler) SetOutLimit(t int, limit int) {
	// NOTE: deprecated
	// scheduler.outLimit[t] = make(chan bool, limit)
	pool, err := scheduler.newOutPool(limit)
	if err != nil {
		panic(err)
	}
	scheduler.outPool[t] = pool
}

func (scheduler *BaseScheduler) getInPool(taskType int) *tunny.Pool {
	pool, ok := scheduler.inPool[taskType]
	if !ok {
		var err error
		pool, err = scheduler.newInPool(runtime.NumCPU())
		if err != nil {
			panic(err)
		}
		scheduler.inPool[taskType] = pool
	}
	return pool
}

func (scheduler *BaseScheduler) getOutPool(taskType int) *tunny.Pool {
	pool, ok := scheduler.outPool[taskType]
	if !ok {
		var err error
		pool, err = scheduler.newOutPool(runtime.NumCPU())
		if err != nil {
			panic(err)
		}
		scheduler.outPool[taskType] = pool
	}
	return pool
}

// NOTE: deprecated
//
// func (scheduler *BaseScheduler) getInLimitChan(t int) chan bool {
// 	limitChan, ok := scheduler.inLimit[t]
// 	if !ok {
// 		limitChan = make(chan bool, runtime.NumCPU()*4)
// 		scheduler.inLimit[t] = limitChan
// 	}
// 	return limitChan
// }

// func (scheduler *BaseScheduler) getOutLimitChan(t int) chan bool {
// 	limitChan, ok := scheduler.outLimit[t]
// 	if !ok {
// 		limitChan = make(chan bool, runtime.NumCPU()*4)
// 		scheduler.outLimit[t] = limitChan
// 	}
// 	return limitChan
// }

func (scheduler *BaseScheduler) SetTaskStore(taskStore TaskStore) {
	scheduler.taskStore = taskStore
	go func() {
		for taskID := range scheduler.in {
			tf, err := scheduler.taskStore.Get(taskID)
			if err != nil {
				log.Errorf("(Task %s) Not Found, Data may be lost.", taskID)
			}
			pool := scheduler.getInPool(int(tf.Input.Type))
			ants.Push(func() {
				pool.Process(tf)
			})
		}
	}()
	go func() {
		for result := range scheduler.out {
			v := result
			pool := scheduler.getOutPool(int(result.Type))
			ants.Push(func() {
				pool.Process(v)
			})
		}
	}()
	go func() {
		for {
			scheduler.checkWait()
			time.Sleep(10 * time.Second)
		}
	}()
}

func (scheduler *BaseScheduler) checkWait() {
	// log.Debugf("%d %d %d", scheduler.count, len(scheduler.in), scheduler.waitChans.Len())
	if scheduler.count == 0 && len(scheduler.in) == 0 {
		scheduler.waitLock.Lock()
		for i := 0; i < scheduler.waitChans.Len(); i++ {
			e := scheduler.waitChans.Front()
			c := e.Value.(chan bool)
			select {
			case c <- true:
				scheduler.waitChans.Remove(e)
			default:
				scheduler.waitChans.MoveToBack(e)
			}
		}
		scheduler.waitLock.Unlock()
	}
}

func (scheduler *BaseScheduler) GetTasks() ([]*task.TaskFuture, error) {
	return scheduler.taskStore.All()
}

func (scheduler *BaseScheduler) Wait(ch chan bool) {
	scheduler.waitLock.Lock()
	defer scheduler.waitLock.Unlock()
	scheduler.waitChans.PushBack(ch)
}

func (scheduler *BaseScheduler) Publish(parent *task.TaskFuture, input task.TaskInput) *task.TaskFuture {
	taskID := scheduler.taskStore.GetNextTaskId()
	now := time.Now()
	tf := &task.TaskFuture{ID: taskID, Input: input, Retry: 0, Status: task.TaskStatusPending, Published: now, Updated: now}
	if parent != nil {
		tf.ParentID = parent.ID
	}
	err := scheduler.taskStore.Set(tf.ID, tf)
	if err != nil {
		log.Errorf("(Task %s) Couldn't store task, aborted", taskID)
		return nil
	}
	scheduler.count++
	scheduler.in <- tf.ID
	return tf
}

var maxRetry = getMaxRetry()

func getMaxRetry() int {
	defaultMaxRetry := 3
	r, err := strconv.Atoi(utils.GetEnv("TASK_MAX_RETRY", strconv.Itoa(defaultMaxRetry)))
	if err != nil {
		return defaultMaxRetry
	}
	return r
}

func getRetryDelay() uint {
	defaultRetryDelay := 10
	r, err := strconv.Atoi(utils.GetEnv("TASK_RETRY_DELAY", strconv.Itoa(defaultRetryDelay)))
	if err != nil || r <= 0 {
		return uint(defaultRetryDelay)
	}
	return uint(r)
}

func getRetryBackoff() bool {
	r := utils.GetEnv("TASK_RETRY_DELAY", "")
	return strings.Compare(r, "") != 0 || strings.Compare(r, "false") != 0
}

func getRetryInterval(times int) uint {
	delay := getRetryDelay()
	backoff := getRetryBackoff()
	if backoff {
		return delay * (1 << (uint(times) - 1))
	} else {
		return delay * uint(times)
	}
}

func (scheduler *BaseScheduler) onFailed(tf *task.TaskFuture, err error) {
	handlers, ok := scheduler.failedHandlers[int(tf.Input.Type)]
	if ok {
		for _, handler := range handlers {
			handler(*tf, err)
		}
	}
	log.Errorf("(Task %s) Failed due to: %s", tf.ID, err)
	wait := time.Duration(getRetryInterval(tf.Retry + 1))
	if tf.Retry < maxRetry {
		log.Infof("(Task %s) Retry for %d time after %d second", tf.ID, tf.Retry+1, wait)
		time.Sleep(wait * time.Second)
		tf.Retry++
		tf.Updated = time.Now()
		tf.Error = err.Error()
		scheduler.taskStore.Set(tf.ID, tf)
		scheduler.in <- tf.ID
	} else {
		log.Errorf("(Task %s) aborted", tf.ID)
		scheduler.taskStore.Remove(tf.ID)
		tf.Status = task.TaskStatusFailed
		tf.Updated = time.Now()
		tf.Error = err.Error()
		scheduler.count--
		scheduler.checkWait()
		handlers, ok := scheduler.abortedHandlers[int(tf.Input.Type)]
		if ok {
			for _, handler := range handlers {
				handler(*tf)
			}
		}
	}
}

func (scheduler *BaseScheduler) onSuccess(output task.TaskResult) {
	tf, err := scheduler.taskStore.Get(output.ID)
	if err != nil {
		log.Errorf("(Task %s) Task not Found", output.ID)
		return
	}
	if output.Error != nil {
		scheduler.onFailed(tf, output.Error)
		return
	}
	handlers, ok := scheduler.successHandlers[int(tf.Input.Type)]
	if ok {
		for _, handler := range handlers {
			err := handler(*tf, output)
			if err != nil {
				scheduler.onFailed(tf, err)
				return
			}
		}
	}
	tf.Status = task.TaskStatusSuccess
	tf.Updated = time.Now()
	scheduler.taskStore.Remove(output.ID)
	scheduler.count--
	scheduler.checkWait()
	postHandlers, ok := scheduler.postSuccessHandlers[int(tf.Input.Type)]
	if ok {
		for _, handler := range postHandlers {
			handler(*tf, output)
		}
	}
}

func (scheduler *BaseScheduler) handle(tf *task.TaskFuture) {
	// defer func() {
	// 	<-scheduler.getInLimitChan(int(tf.Input.Type))
	// }()
	err := scheduler.taskStore.Set(tf.ID, tf)
	if err != nil {
		scheduler.onFailed(tf, err)
		return
	}
	handers, ok := scheduler.handlers[int(tf.Input.Type)]
	if ok {
		for _, handler := range handers {
			err := handler(tf)
			if err != nil {
				scheduler.onFailed(tf, err)
				break
			}
		}
	}
}

func (scheduler *BaseScheduler) AppendHandler(t int, handler func(*task.TaskFuture) error) {
	handlers, ok := scheduler.handlers[t]
	if !ok {
		handlers = make([]func(*task.TaskFuture) error, 0)
	}
	scheduler.handlers[t] = append(handlers, handler)
}

func (scheduler *BaseScheduler) PrependHandler(t int, handler func(*task.TaskFuture) error) {
	handlers, ok := scheduler.handlers[t]
	if !ok {
		handlers = make([]func(*task.TaskFuture) error, 0)
	}
	scheduler.handlers[t] = append([]func(*task.TaskFuture) error{handler}, handlers...)
}

func (scheduler *BaseScheduler) AppendSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult) error) {
	handlers, ok := scheduler.successHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture, task.TaskResult) error, 0)
	}
	scheduler.successHandlers[t] = append(handlers, handler)
}

func (scheduler *BaseScheduler) PrependSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult) error) {
	handlers, ok := scheduler.successHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture, task.TaskResult) error, 0)
	}
	scheduler.successHandlers[t] = append([]func(task.TaskFuture, task.TaskResult) error{handler}, handlers...)
}

func (scheduler *BaseScheduler) AppendFailureHandler(t int, handler func(task.TaskFuture, error)) {
	handlers, ok := scheduler.failedHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture, error), 0)
	}
	scheduler.failedHandlers[t] = append(handlers, handler)
}

func (scheduler *BaseScheduler) PrependFailureHandler(t int, handler func(task.TaskFuture, error)) {
	handlers, ok := scheduler.failedHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture, error), 0)
	}
	scheduler.failedHandlers[t] = append([]func(task.TaskFuture, error){handler}, handlers...)
}

func (scheduler *BaseScheduler) AppendPostSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult)) {
	handlers, ok := scheduler.postSuccessHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture, task.TaskResult), 0)
	}
	scheduler.postSuccessHandlers[t] = append(handlers, handler)
}

func (scheduler *BaseScheduler) PrependPostSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult)) {
	handlers, ok := scheduler.postSuccessHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture, task.TaskResult), 0)
	}
	scheduler.postSuccessHandlers[t] = append([]func(task.TaskFuture, task.TaskResult){handler}, handlers...)
}

func (scheduler *BaseScheduler) AppendAbortedHandler(t int, handler func(task.TaskFuture)) {
	handlers, ok := scheduler.abortedHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture), 0)
	}
	scheduler.abortedHandlers[t] = append(handlers, handler)
}

func (scheduler *BaseScheduler) PrependAbortedHandler(t int, handler func(task.TaskFuture)) {
	handlers, ok := scheduler.abortedHandlers[t]
	if !ok {
		handlers = make([]func(task.TaskFuture), 0)
	}
	scheduler.abortedHandlers[t] = append([]func(task.TaskFuture){handler}, handlers...)
}

func (scheduler *BaseScheduler) Close() {
	// NOTE: deprecated
	// for _, v := range scheduler.inLimit {
	// 	close(v)
	// }
	// for _, v := range scheduler.outLimit {
	// 	close(v)
	// }
	for _, v := range scheduler.inPool {
		v.Close()
	}
	for _, v := range scheduler.outPool {
		v.Close()
	}
	close(scheduler.in)
	close(scheduler.out)
}
