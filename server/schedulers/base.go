package schedulers

import (
	"container/list"
	"runtime"
	"strconv"
	"sync"
	"time"

	"fxdayu.com/dyupdater/server/common"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
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
	handle(tf *task.TaskFuture)
	onFailed(tf *task.TaskFuture, err error)
	onSuccess(output task.TaskResult)
	SetTaskStore(TaskStore)
	Publish(parent *task.TaskFuture, input task.TaskInput) *task.TaskFuture
	SetInLimit(t int, limit int)
	SetOutLimit(t int, limit int)
	GetTasks() ([]*task.TaskFuture, error)
	AppendHandler(t int, handler func(*task.TaskFuture) error)
	AppendSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult) error)
	AppendFailureHandler(t int, handler func(task.TaskFuture, error))
	PrependHandler(t int, handler func(*task.TaskFuture) error)
	PrependSuccessHandler(t int, handler func(task.TaskFuture, task.TaskResult) error)
	PrependFailureHandler(t int, handler func(task.TaskFuture, error))
	GetOutputChan() chan task.TaskResult
	Wait(chan bool)
}

type BaseScheduler struct {
	taskStore       TaskStore
	handlers        map[int][]func(*task.TaskFuture) error
	successHandlers map[int][]func(task.TaskFuture, task.TaskResult) error
	failedHandlers  map[int][]func(task.TaskFuture, error)
	in              chan string
	out             chan task.TaskResult
	inLimit         map[int]chan bool
	outLimit        map[int]chan bool
	count           int
	waitChans       *list.List
	waitLock        *sync.Mutex
}

func (scheduler *BaseScheduler) Init(config *viper.Viper) {
	scheduler.handlers = make(map[int][]func(*task.TaskFuture) error, 0)
	scheduler.successHandlers = make(map[int][]func(task.TaskFuture, task.TaskResult) error, 0)
	scheduler.failedHandlers = make(map[int][]func(task.TaskFuture, error), 0)
	scheduler.inLimit = make(map[int]chan bool)
	scheduler.outLimit = make(map[int]chan bool)
	scheduler.in = make(chan string, 100000)
	scheduler.out = make(chan task.TaskResult, 100000)
	scheduler.waitLock = &sync.Mutex{}
	scheduler.waitChans = list.New()
}

func (scheduler *BaseScheduler) GetOutputChan() chan task.TaskResult {
	return scheduler.out
}

func (scheduler *BaseScheduler) SetInLimit(t int, limit int) {
	scheduler.inLimit[t] = make(chan bool, limit)
}

func (scheduler *BaseScheduler) SetOutLimit(t int, limit int) {
	scheduler.outLimit[t] = make(chan bool, limit)
}

func (scheduler *BaseScheduler) getInLimitChan(t int) chan bool {
	limitChan, ok := scheduler.inLimit[t]
	if !ok {
		limitChan = make(chan bool, runtime.NumCPU()*4)
		scheduler.inLimit[t] = limitChan
	}
	return limitChan
}

func (scheduler *BaseScheduler) getOutLimitChan(t int) chan bool {
	limitChan, ok := scheduler.outLimit[t]
	if !ok {
		limitChan = make(chan bool, runtime.NumCPU()*4)
		scheduler.outLimit[t] = limitChan
	}
	return limitChan
}

func (scheduler *BaseScheduler) SetTaskStore(taskStore TaskStore) {
	scheduler.taskStore = taskStore
	go func() {
		for taskID := range scheduler.in {
			tf, err := scheduler.taskStore.Get(taskID)
			if err != nil {
				log.Errorf("(Task %s) Not Found, Data may be lost.", taskID)
			}
			scheduler.getInLimitChan(int(tf.Input.Type)) <- true
			go scheduler.handle(tf)
		}
	}()
	go func() {
		for result := range scheduler.out {
			scheduler.getOutLimitChan(int(result.Type)) <- true
			go scheduler.onSuccess(result)
		}
	}()
}

func (scheduler *BaseScheduler) checkWait() {
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
	r, err := strconv.Atoi(utils.GetEnv("TASK_MAX_RETRY", "3"))
	if err != nil {
		return 3
	}
	return r
}

func (scheduler *BaseScheduler) onFailed(tf *task.TaskFuture, err error) {
	handlers, ok := scheduler.failedHandlers[int(tf.Input.Type)]
	if ok {
		for _, handler := range handlers {
			handler(*tf, err)
		}
	}
	log.Errorf("(Task %s) Failed due to: %s", tf.ID, err)
	wait := time.Duration((tf.Retry + 1) * 5)
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
	}
}

func (scheduler *BaseScheduler) onSuccess(output task.TaskResult) {
	defer func() {
		<-scheduler.getOutLimitChan(int(output.Type))
	}()
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
}

func (scheduler *BaseScheduler) handle(tf *task.TaskFuture) {
	defer func() {
		<-scheduler.getInLimitChan(int(tf.Input.Type))
	}()
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

func (scheduler *BaseScheduler) Close() {
	for _, v := range scheduler.inLimit {
		close(v)
	}
	for _, v := range scheduler.outLimit {
		close(v)
	}
	close(scheduler.in)
	close(scheduler.out)
}
