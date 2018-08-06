package task

import (
	"fmt"
)

type TaskNotFoundError struct {
	id string
}

func (e TaskNotFoundError) Error() string {
	return fmt.Sprintf("task not found: %s", e.id)
}

func (e TaskNotFoundError) GetID() string {
	return e.id
}

func NewTaskNotFoundError(id string) TaskNotFoundError {
	return TaskNotFoundError{id: id}
}
