package schedulers

import (
	"errors"
	"strconv"
	"sync"

	"github.com/spf13/viper"
	"fxdayu.com/dyupdater/server/task"
)

type InMemoryTaskStore struct {
	taskID int
	tasks  map[string]*task.TaskFuture
	lock   sync.RWMutex
}

func (this *InMemoryTaskStore) Init() {
	this.tasks = make(map[string]*task.TaskFuture)
	this.lock = sync.RWMutex{}
}

func (this *InMemoryTaskStore) GetNextTaskId() string {
	this.taskID++
	return strconv.Itoa(this.taskID)
}

func (this *InMemoryTaskStore) Set(id string, task *task.TaskFuture) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.tasks[id] = task
	return nil
}

func (this *InMemoryTaskStore) Get(id string) (*task.TaskFuture, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	r, ok := this.tasks[id]
	if !ok {
		return nil, errors.New("Not Found Task " + id)
	}
	return r, nil
}

func (this *InMemoryTaskStore) Remove(id string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.tasks, id)
	return nil
}

func (this *InMemoryTaskStore) All() ([]*task.TaskFuture, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	r := make([]*task.TaskFuture, 0, len(this.tasks))
	for _, v := range this.tasks {
		r = append(r, v)
	}
	return r, nil
}

type InMemoryTaskScheduler struct {
	BaseScheduler
}

func (this *InMemoryTaskScheduler) Init(config *viper.Viper) {
	this.BaseScheduler.Init(config)
	store := new(InMemoryTaskStore)
	store.Init()
	this.SetTaskStore(store)
}
