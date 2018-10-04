package task

import "fxdayu.com/dyupdater/server/models"

type UpdateTaskPayload struct {
	BaseFactorTaskPayload
	Store       string
	FactorValue models.FactorValue
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

func NewUpdateTaskPayload(store string, factor models.Factor, factorValue models.FactorValue, processType FactorProcessType) UpdateTaskPayload {
	return UpdateTaskPayload{BaseFactorTaskPayload: BaseFactorTaskPayload{Factor: factor, ProcessType: processType}, Store: store, FactorValue: factorValue}
}

func NewUpdateTaskInput(store string, factor models.Factor, factorValue models.FactorValue, processType FactorProcessType) TaskInput {
	return TaskInput{Type: TaskTypeUpdate, Payload: NewUpdateTaskPayload(store, factor, factorValue, processType)}
}

type UpdateTaskResult struct {
	Count int
}
