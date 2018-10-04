package task

// TaskType is the type of the task.
type TaskType int

// The enum of TaskType
const (
	TaskTypeCheck TaskType = iota
	TaskTypeCal
	TaskTypeUpdate
	TaskTypeProcess
)

// The enum of TaskStatus
const (
	TaskStatusPending int = iota
	TaskStatusSuccess
	TaskStatusFailed
)

// FactorProcessType means post process type of factor
type FactorProcessType string

const (
	//ProcessTypeNone means raw value
	ProcessTypeNone FactorProcessType = ""
	//ProcessTypeWinsorize means winsorize
	ProcessTypeWinsorize FactorProcessType = "W"
	//ProcessTypeStandardize means standardize
	ProcessTypeStandardize FactorProcessType = "S"
	//ProcessTypeAll means winsorize then standardize
	ProcessTypeAll FactorProcessType = "A"
)
