package definition

import (
	"fmt"
	"strings"
)

// TaskItem represents definition of partition in scheduling.
// A legal Worker must be assigned at least 1 TaskItem.
type TaskItem struct {
	Id        string
	Parameter string
}

func (item *TaskItem) String() string {
	return fmt.Sprint("(t=", item.Id, ",p=", item.Parameter, ")")
}

type TaskAssignment struct {
	StrategyId         string
	TaskId             string
	ItemId             string
	RuntimeId          string
	RequestedRuntimeId string
	Parameter          string
}

func (assign *TaskAssignment) String() string {
	b := strings.Builder{}
	b.WriteRune('{')
	b.WriteString(assign.ItemId)
	b.WriteString(" => ")
	if assign.RuntimeId == "" && assign.RequestedRuntimeId == "" {
		b.WriteString("<empty>")
	}
	if assign.RuntimeId != "" {
		b.WriteString(assign.RuntimeId)
	} else if assign.RequestedRuntimeId != "" {
		b.WriteRune('*')
		b.WriteString(assign.RequestedRuntimeId)
	}
	b.WriteRune('}')
	return b.String()
}

type TaskRuntime struct {
	Id            string
	Version       int64
	CreateTime    int64
	LastHeartBeat int64
	NextRunnable  int64 // Zero indicating running
	Statistics    Statistics

	// Redundant fields which can be verified on console or other tools
	Ip            string
	Hostname      string
	ExecutorCount int
	SchedulerId   string
	StrategyId    string
	OwnSign       string
	TaskId        string
	Bind          string
}
