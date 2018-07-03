package utils

import (
	"sync"

	"github.com/deckarep/golang-set"

	"fxdayu.com/dyupdater/server/task"
	lane "gopkg.in/oleiade/lane.v1"
)

type TaskArchive struct {
	maxLen int
	deque  *lane.Deque
	set    mapset.Set
	lock   *sync.RWMutex
}

func (archive *TaskArchive) Append(tf *task.TaskFuture) {
	archive.lock.Lock()
	defer archive.lock.Unlock()
	if !archive.set.Contains(tf) {
		if archive.deque.Size() >= archive.maxLen {
			archive.deque.Shift()
		}
		archive.deque.Append(tf)
		archive.set.Add(tf)
	}
}

func (archive *TaskArchive) All() []*task.TaskFuture {
	archive.lock.RLock()
	defer archive.lock.RUnlock()
	n := archive.deque.Size()
	r := make([]*task.TaskFuture, 0, n)
	if n > 0 {
		for i := 0; i < n; i++ {
			v := archive.deque.Shift().(*task.TaskFuture)
			if v != nil {
				r = append(r, v)
				archive.deque.Append(v)
			}
		}
	}
	return r
}

func NewTaskArchive() *TaskArchive {
	archive := &TaskArchive{}
	archive.maxLen = 10000
	archive.deque = lane.NewDeque()
	archive.set = mapset.NewSet()
	archive.lock = &sync.RWMutex{}
	return archive
}

var archive *TaskArchive

func GetTaskArchive() *TaskArchive {
	if archive == nil {
		archive = NewTaskArchive()
	}
	return archive
}
