package services

import (
	"errors"
	"fmt"

	"fxdayu.com/dyupdater/server/task"
)

type updateTaskHandler struct {
	service *FactorServices
}

func (handler *updateTaskHandler) Handle(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.UpdateTaskPayload)
	if !ok {
		return errors.New("Unvalid update task")
	}
	log.Infof("(Task %s) { %s }  [ %d , %d ] Update begin.", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	service := handler.service
	store, ok := service.stores[data.Store]
	if !ok {
		return fmt.Errorf("Store not Found: %s", data.Store)
	}
	count, err := store.Update(service.mapFactor(data.Store, data.Factor), data.ProcessType, data.FactorValue, false)
	if err != nil {
		return err
	}
	output := new(task.TaskResult)
	result := task.UpdateTaskResult{Count: count}
	output.ID = tf.ID
	output.Type = task.TaskTypeUpdate
	output.Result = result
	out := service.scheduler.GetOutputChan(int(output.Type))
	out <- *output
	return nil
}

func (handler *updateTaskHandler) OnSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.UpdateTaskResult)
	if !ok {
		return errors.New("Unvalid update result")
	}
	data := tf.Input.Payload.(task.UpdateTaskPayload)
	log.Infof("(Task %s) { %s } [ %d , %d ] Update finish. %d record was updated.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), result.Count)
	return nil
}

func (handler *updateTaskHandler) OnFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.UpdateTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Update failed in store %s: %s", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), data.Store, err)
}

func (handler *updateTaskHandler) GetTaskType() task.TaskType {
	return task.TaskTypeUpdate
}

func newUpdateTaskHandler(service *FactorServices) *updateTaskHandler {
	return &updateTaskHandler{service: service}
}
