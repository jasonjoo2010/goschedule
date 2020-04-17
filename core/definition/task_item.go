package definition

import "fmt"

// TaskItem represents definition of partition in scheduling.
// A legal Worker must be assigned at least 1 TaskItem.
type TaskItem struct {
	id        string
	parameter string
}

func (item *TaskItem) String() string {
	return fmt.Sprint("(t=", item.id, ",p=", item.parameter, ")")
}
