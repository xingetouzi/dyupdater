package task

// TaskType is the type of the task.
type TaskType int

// The enum of TaskType
const (
	TaskTypeCheck TaskType = iota
	TaskTypeCal
	TaskTypeUpdate
)

// The enum of TaskStatus
const (
	TaskStatusPending int = iota
	TaskStatusSuccess
	TaskStatusFailed
)
