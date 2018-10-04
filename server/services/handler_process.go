package services

import (
	"errors"
	"fmt"

	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
)

type processTaskHandler struct {
	service *FactorServices
}

func (handler *processTaskHandler) Handle(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.ProcessTaskPayload)
	if !ok {
		return errors.New("Unvalid process task")
	}
	calculator, ok := handler.service.calculators[data.Calculator]
	if !ok {
		return fmt.Errorf("calculator not found: %s", data.Calculator)
	}
	log.Infof("(Task %s) { %s }  [ %d , %d ] Process begin.", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	err := calculator.Process(tf.ID, data.Factor, data.FactorValue, data.ProcessType)
	if err != nil {
		return err
	}
	return nil
}

func (handler *processTaskHandler) OnSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.ProcessTaskResult)
	if !ok {
		return errors.New("Unvalid process result")
	}
	data := tf.Input.Payload.(task.ProcessTaskPayload)
	log.Infof("(Task %s) { %s } [ %d , %d ] Process finish.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	syncFrom := utils.GetGlobalConfig().GetSyncFrom()
	for name, store := range handler.service.stores {
		if store.IsEnabled() && name != syncFrom {
			ti := task.NewUpdateTaskInput(name, data.Factor, result.FactorValue, data.GetProcessType())
			handler.service.scheduler.Publish(&tf, ti)
		}
	}
	return nil
}

func (handler *processTaskHandler) OnFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.ProcessTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Process failed: %s", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), err)
}

func (handler *processTaskHandler) GetTaskType() task.TaskType {
	return task.TaskTypeProcess
}

func newProcessTaskHandler(service *FactorServices) *processTaskHandler {
	return &processTaskHandler{service: service}
}
