package task

import (
	"fmt"
	"time"

	"fxdayu.com/dyupdater/server/models"
)

type TaskInput struct {
	Type    TaskType
	Payload interface{}
}

type TaskResult struct {
	ID     string
	Type   TaskType
	Result interface{}
	Error  error
}

type TaskFuture struct {
	ID        string
	ParentID  string
	Input     TaskInput
	Retry     int
	Status    int
	Published time.Time
	Updated   time.Time
	Error     string
}

func (this *TaskFuture) Wait(timeout time.Duration) bool {
	ch := make(chan bool)
	go func() {
		for this.Status == TaskStatusPending {
			time.Sleep(0)
		}
		ch <- true
	}()
	defer close(ch)
	if timeout <= 0 {
		return <-ch
	}
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		close(ch)
		return false
	}
}

type FactorTaskPayload interface {
	GetFactorID() string
	GetStartTime() int
	GetEndTime() int
	GetProcessType() FactorProcessType
}

type BaseFactorTaskPayload struct {
	Factor      models.Factor
	ProcessType FactorProcessType
}

func (payload BaseFactorTaskPayload) GetFactorID() string {
	factorID := payload.Factor.Name
	if payload.ProcessType != ProcessTypeNone {
		factorID += fmt.Sprintf("[%s]", string(payload.ProcessType))
	}
	return factorID
}

func (payload BaseFactorTaskPayload) GetProcessType() FactorProcessType {
	return payload.ProcessType
}
