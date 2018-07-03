package task

import (
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
}

type CheckTaskPayload struct {
	Stores    []string
	Factor    models.Factor
	DateRange models.DateRange
}

func (checkTaskPayload CheckTaskPayload) GetFactorID() string {
	return checkTaskPayload.Factor.ID
}

func (checkTaskPayload CheckTaskPayload) GetStartTime() int {
	return checkTaskPayload.DateRange.Start
}

func (checkTaskPayload CheckTaskPayload) GetEndTime() int {
	return checkTaskPayload.DateRange.End
}

type CalTaskPayload struct {
	Calculator string
	Factor     models.Factor
	DateRange  models.DateRange
}

func (calTaskPayload CalTaskPayload) GetFactorID() string {
	return calTaskPayload.Factor.ID
}

func (calTaskPayload CalTaskPayload) GetStartTime() int {
	return calTaskPayload.DateRange.Start
}

func (calTaskPayload CalTaskPayload) GetEndTime() int {
	return calTaskPayload.DateRange.End
}

type UpdateTaskPayload struct {
	Store       string
	Factor      models.Factor
	FactorValue models.FactorValue
}

func (updateTaskPayload UpdateTaskPayload) GetFactorID() string {
	return updateTaskPayload.Factor.ID
}

func (updateTaskPayload UpdateTaskPayload) GetStartTime() int {
	if len(updateTaskPayload.FactorValue.Datetime) == 0 {
		return 0
	}
	return updateTaskPayload.FactorValue.Datetime[0]
}

func (updateTaskPayload UpdateTaskPayload) GetEndTime() int {
	l := len(updateTaskPayload.FactorValue.Datetime)
	if l == 0 {
		return 0
	}
	return updateTaskPayload.FactorValue.Datetime[l-1]
}

type CalTaskResult struct {
	FactorValue models.FactorValue
}

type CheckTaskResult struct {
	Datetimes []int
}
type UpdateTaskResult struct {
	Count int
}
