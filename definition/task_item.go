// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package definition

import (
	"fmt"
	"strings"
)

// TaskItem represents definition of partition in scheduling.
// A legal Worker must be assigned at least 1 TaskItem.
type TaskItem struct {
	ID        string
	Parameter string
}

func (item *TaskItem) String() string {
	return fmt.Sprint("(t=", item.ID, ",p=", item.Parameter, ")")
}

type TaskAssignment struct {
	StrategyID         string
	TaskID             string
	ItemID             string
	RuntimeID          string
	RequestedRuntimeID string
	Parameter          string
}

func (assign *TaskAssignment) String() string {
	b := strings.Builder{}
	b.WriteRune('{')
	b.WriteString(assign.ItemID)
	b.WriteString(" => ")
	if assign.RuntimeID == "" && assign.RequestedRuntimeID == "" {
		b.WriteString("<empty>")
	}
	if assign.RuntimeID != "" {
		b.WriteString(assign.RuntimeID)
	} else if assign.RequestedRuntimeID != "" {
		b.WriteRune('*')
		b.WriteString(assign.RequestedRuntimeID)
	}
	b.WriteRune('}')
	return b.String()
}

type TaskRuntime struct {
	ID            string
	Version       int64
	Createtime    int64
	LastHeartbeat int64
	NextRunnable  int64 // Zero indicating running
	Statistics    Statistics

	// Redundant fields which can be verified on console or other tools
	IP            string
	Hostname      string
	ExecutorCount int
	SchedulerID   string
	StrategyID    string
	OwnSign       string
	TaskID        string
	Bind          string
}
