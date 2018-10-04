package task

import "fxdayu.com/dyupdater/server/models"

type CheckTaskPayload struct {
	BaseFactorTaskPayload
	Stores    []string
	DateRange models.DateRange
}

func (checkTaskPayload CheckTaskPayload) GetStartTime() int {
	return checkTaskPayload.DateRange.Start
}

func (checkTaskPayload CheckTaskPayload) GetEndTime() int {
	return checkTaskPayload.DateRange.End
}

func NewCheckTaskPayload(stores []string, factor models.Factor, dateRange models.DateRange, processType FactorProcessType) CheckTaskPayload {
	return CheckTaskPayload{BaseFactorTaskPayload: BaseFactorTaskPayload{Factor: factor, ProcessType: processType}, Stores: stores, DateRange: dateRange}
}

func NewCheckTaskInput(stores []string, factor models.Factor, dateRange models.DateRange, processType FactorProcessType) TaskInput {
	input := TaskInput{Type: TaskTypeCheck, Payload: NewCheckTaskPayload(stores, factor, dateRange, processType)}
	return input
}

type CheckTaskResult struct {
	Datetimes []int
}
