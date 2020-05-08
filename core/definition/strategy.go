package definition

import (
	"encoding/json"
)

type StrategyKind int

const (
	_ StrategyKind = iota
	GeneralKind
	TaskKind
)

type Strategy struct {
	Id                   string
	IpList               []string // Which can be scheduled on
	MaxOnSingleScheduler int      // Max workers can be created on the same scheduler
	Total                int      // Total workers should be created
	Kind                 StrategyKind
	Parameter            string // Will be ignored if kind == Task
	TaskId               string
	Enabled              bool // Whether it should begin to schedule
}

func (s *Strategy) String() string {
	data, _ := json.Marshal(s)
	return string(data)
}

type StrategyRuntime struct {
	SchedulerId  string
	StrategyId   string
	Num          int
	RequestedNum int
}

func (s *StrategyRuntime) String() string {
	data, _ := json.Marshal(s)
	return string(data)
}
