package utils

import (
	"sync"

	"fxdayu.com/dyupdater/server/task"
	lane "gopkg.in/oleiade/lane.v1"
)

type TaskArchiveRecord struct {
	ID        string `json:"id"`
	ParentID  string `json:"parent"`
	TaskType  int    `json:"type"`
	Retry     int    `json:"retry"`
	Factor    string `json:"factor"`
	Start     int    `json:"start"`
	End       int    `json:"end"`
	Status    int    `json:"status"`
	Published string `json:"published"`
	Updated   string `json:"updated"`
	Error     string `json:"error"`
}

func NewTaskArchiveRecord(tf *task.TaskFuture) (tr TaskArchiveRecord) {
	tr = TaskArchiveRecord{
		ID:        tf.ID,
		ParentID:  tf.ParentID,
		TaskType:  int(tf.Input.Type),
		Retry:     tf.Retry,
		Status:    tf.Status,
		Published: tf.Published.Format("06/01/02 15:04:05"),
		Updated:   tf.Updated.Format("06/01/02 15:04:05"),
		Error:     tf.Error,
	}
	payload := tf.Input.Payload.(task.FactorTaskPayload)
	tr.Factor = payload.GetFactorID()
	tr.Start = payload.GetStartTime()
	tr.End = payload.GetEndTime()
	return
}

type TaskArchive struct {
	maxLen int
	deque  *lane.Deque
	set    map[string]TaskArchiveRecord
	lock   *sync.RWMutex
}

func (archive *TaskArchive) Set(tr TaskArchiveRecord) {
	archive.lock.Lock()
	defer archive.lock.Unlock()
	if _, ok := archive.set[tr.ID]; !ok {
		if archive.deque.Size() >= archive.maxLen {
			tID := archive.deque.Shift().(string)
			delete(archive.set, tID)
		}
		archive.deque.Append(tr.ID)
	}
	archive.set[tr.ID] = tr
}

func (archive *TaskArchive) All() []TaskArchiveRecord {
	archive.lock.RLock()
	defer archive.lock.RUnlock()
	m := archive.set
	v := make([]TaskArchiveRecord, 0, len(m))
	for _, value := range m {
		v = append(v, value)
	}
	return v
}

func NewTaskArchive() *TaskArchive {
	archive := &TaskArchive{}
	archive.maxLen = 10000
	archive.deque = lane.NewDeque()
	archive.set = make(map[string]TaskArchiveRecord)
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
