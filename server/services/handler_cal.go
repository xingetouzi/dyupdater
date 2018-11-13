package services

import (
	"errors"
	"fmt"
	"sort"

	"fxdayu.com/dyupdater/server/models"
	"fxdayu.com/dyupdater/server/task"
	"fxdayu.com/dyupdater/server/utils"
)

type calTaskHandler struct {
	service *FactorServices
}

func (handler *calTaskHandler) Handle(tf *task.TaskFuture) error {
	input := tf.Input
	data, ok := input.Payload.(task.CalTaskPayload)
	if !ok {
		return errors.New("Unvalid cal task")
	}
	service := handler.service
	syncFrom := utils.GetGlobalConfig().GetSyncFrom()
	if syncFrom != "" {
		store, ok := service.stores[syncFrom]
		if !ok {
			panic(fmt.Errorf("store not found: %s", syncFrom))
		}
		log.Infof("(Task %s) { %s }  [ %d , %d ] Fetch begin.", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
		if values, err := store.Fetch(service.mapFactor(syncFrom, data.Factor), data.DateRange); err != nil {
			return err
		} else {
			output := new(task.TaskResult)
			result := task.CalTaskResult{FactorValue: values}
			output.ID = tf.ID
			output.Type = task.TaskTypeCal
			output.Result = result
			out := service.scheduler.GetOutputChan(int(output.Type))
			out <- *output
			return nil
		}
	}
	calculator, ok := service.calculators[data.Calculator]
	if !ok {
		return fmt.Errorf("calculator not found: %s", data.Calculator)
	}
	log.Infof("(Task %s) { %s }  [ %d , %d ] Cal begin.", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	err := calculator.Cal(tf.ID, data.Factor, data.DateRange)
	if err != nil {
		return err
	}
	return nil
}

func (handler *calTaskHandler) OnSuccess(tf task.TaskFuture, r task.TaskResult) error {
	result, ok := r.Result.(task.CalTaskResult)
	if !ok {
		return errors.New("invalid cal result")
	}
	data := tf.Input.Payload.(task.CalTaskPayload)
	log.Infof("(Task %s) { %s } [ %d , %d ] Cal finish.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
	// trunc cal result
	service := handler.service
	calStartDate := utils.GetGlobalConfig().GetCalStartDate()
	factorValue := models.FactorValue{}
	if data.DateRange.Start > calStartDate {
		calStartDate = data.DateRange.Start
	}
	startIndex := sort.SearchInts(result.FactorValue.Datetime, calStartDate)
	endIndex := sort.Search(len(result.FactorValue.Datetime), func(i int) bool { return result.FactorValue.Datetime[i] > data.DateRange.End })
	if startIndex >= len(result.FactorValue.Datetime) {
		log.Infof("(Task %s) { %s } [ %d , %d ] No data to update.", r.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime())
		return nil
	}
	factorValue.Datetime = result.FactorValue.Datetime[startIndex:endIndex]
	factorValue.Values = map[string][]float64{}
	for k, v := range result.FactorValue.Values {
		factorValue.Values[k] = v[startIndex:endIndex]
	}
	// update cal result
	syncFrom := utils.GetGlobalConfig().GetSyncFrom()
	for name, store := range service.stores {
		if store.IsEnabled() && name != syncFrom {
			ti := task.NewUpdateTaskInput(name, data.Factor, factorValue, task.ProcessTypeNone)
			service.scheduler.Publish(&tf, ti)
		}
	}
	return nil
}

func (handler *calTaskHandler) OnFailed(tf task.TaskFuture, err error) {
	input := tf.Input
	data, ok := input.Payload.(task.CalTaskPayload)
	if !ok {
		return
	}
	log.Errorf("(Task %s) { %s } [ %d , %d ] Cal failed: %s", tf.ID, data.GetFactorID(), data.GetStartTime(), data.GetEndTime(), err)
}

func (handler *calTaskHandler) GetTaskType() task.TaskType {
	return task.TaskTypeCal
}

func newCalTaskHandler(service *FactorServices) *calTaskHandler {
	return &calTaskHandler{service: service}
}
