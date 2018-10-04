package task

import "fxdayu.com/dyupdater/server/models"

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

func (CalTaskPayload CalTaskPayload) GetProcessType() FactorProcessType {
	return ProcessTypeNone
}

func NewCalTaskPayload(calculator string, factor models.Factor, dateRange models.DateRange) CalTaskPayload {
	return CalTaskPayload{Calculator: calculator, Factor: factor, DateRange: dateRange}
}

func NewCalTaskInput(calculator string, factor models.Factor, dateRange models.DateRange) TaskInput {
	input := TaskInput{Type: TaskTypeCal, Payload: NewCalTaskPayload(calculator, factor, dateRange)}
	return input
}

type CalTaskResult struct {
	FactorValue models.FactorValue
}
