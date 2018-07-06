package celery

import (
	"fmt"
	"sync"
)

type taskInfo struct {
	taskID    string
	routerKey string
}

type taskMapStore interface {
	Get(id string) (*taskInfo, error)
	Set(id string, value *taskInfo)
	IsSet(id string) bool
	Remove(id string)
	Iter() []string
}

type inMemoryTaskMapStore struct {
	tasks map[string]*taskInfo
	lock  sync.RWMutex
}

func newInMemoryTaskMapStore() *inMemoryTaskMapStore {
	r := new(inMemoryTaskMapStore)
	r.tasks = make(map[string]*taskInfo)
	r.lock = sync.RWMutex{}
	return r
}

func (taskStore *inMemoryTaskMapStore) Get(id string) (*taskInfo, error) {
	taskStore.lock.RLock()
	defer taskStore.lock.RUnlock()
	r, ok := taskStore.tasks[id]
	if !ok {
		return nil, fmt.Errorf("No cal task with id (%s) found", id)
	}
	return r, nil
}

func (taskStore *inMemoryTaskMapStore) Set(id string, value *taskInfo) {
	taskStore.lock.Lock()
	defer taskStore.lock.Unlock()
	taskStore.tasks[id] = value
}

func (taskStore *inMemoryTaskMapStore) Remove(id string) {
	taskStore.lock.Lock()
	defer taskStore.lock.Unlock()
	delete(taskStore.tasks, id)
}

func (taskStore *inMemoryTaskMapStore) IsSet(id string) bool {
	taskStore.lock.RLock()
	taskStore.lock.RUnlock()
	_, ok := taskStore.tasks[id]
	return ok
}

func (taskStore *inMemoryTaskMapStore) Iter() []string {
	taskStore.lock.RLock()
	defer taskStore.lock.RUnlock()
	r := make([]string, len(taskStore.tasks))
	var i int
	for k := range taskStore.tasks {
		r[i] = k
		i++
	}
	return r
}
