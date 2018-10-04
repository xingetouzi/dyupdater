package task

import "fxdayu.com/dyupdater/server/models"

type ProcessTaskPayload struct {
	BaseFactorTaskPayload
	Calculator  string
	FactorValue models.FactorValue
}

func (processTaskPayload ProcessTaskPayload) GetStartTime() int {
	if len(processTaskPayload.FactorValue.Datetime) == 0 {
		return 0
	}
	return processTaskPayload.FactorValue.Datetime[0]
}

func (processTaskPayload ProcessTaskPayload) GetEndTime() int {
	l := len(processTaskPayload.FactorValue.Datetime)
	if l == 0 {
		return 0
	}
	return processTaskPayload.FactorValue.Datetime[l-1]
}

type ProcessTaskResult struct {
	FactorValue models.FactorValue
}

func NewProcessTaskPayload(calculator string, factor models.Factor, factorValue models.FactorValue, processType FactorProcessType) ProcessTaskPayload {
	return ProcessTaskPayload{BaseFactorTaskPayload: BaseFactorTaskPayload{Factor: factor, ProcessType: processType}, Calculator: calculator, FactorValue: factorValue}
}

func NewProcessTaskInput(calculator string, factor models.Factor, factorValue models.FactorValue, processType FactorProcessType) TaskInput {
	input := TaskInput{Type: TaskTypeProcess, Payload: NewProcessTaskPayload(calculator, factor, factorValue, processType)}
	return input
}
