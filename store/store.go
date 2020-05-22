package store

import (
	"errors"

	"github.com/jasonjoo2010/enhanced-utils/concurrent/distlock"
	"github.com/jasonjoo2010/goschedule/core/definition"
)

// Period of validity for objects:
// Task - persistent
// Strategy - persistent
// StrategyRuntime - temporary
// Scheduler - temporary (And it has death detection)

// The consistence and correctness should be guaranteed in upper layer which
// can make the store layer much easiler

var (
	NotExist     = errors.New("Specified item is not existed")
	AlreadyExist = errors.New("Specified item is already existed")
)

type Store interface {

	// Return the storage name which can identify the implementation
	Name() string

	// Return the timestamp on storage server side, in millisecond
	// This will help nodes use the consistent time avoiding possible inconsistent between worker nodes.
	// Note: It should be efficient enough to be called frequently and concurrently.
	Time() int64

	// Return a increasing ONLY, NEVER duplicated, GLOBAL distinct (in distribution scenario) sequence number.
	Sequence() (uint64, error)

	// Close the storage and it will never be use again
	Close() error

	Lock() *distlock.DistLock

	// scheduler
	RegisterScheduler(scheduler *definition.Scheduler) error
	UnregisterScheduler(id string) error
	// GetSchedulers returns all the schedulers and sorted by scheduler's id base on sequence number in ascent
	GetSchedulers() ([]*definition.Scheduler, error)
	GetScheduler(id string) (*definition.Scheduler, error)

	// GetTask returns the specified task if it exists or nil with an error of NotExist
	GetTask(id string) (*definition.Task, error)
	GetTasks() ([]*definition.Task, error)
	CreateTask(task *definition.Task) error
	UpdateTask(task *definition.Task) error
	DeleteTask(id string) error

	// task runtimes
	GetTaskRuntime(taskId, id string) (*definition.TaskRuntime, error)
	GetTaskRuntimes(taskId string) ([]*definition.TaskRuntime, error)
	SetTaskRuntime(runtime *definition.TaskRuntime) error
	RemoveTaskRuntime(taskId, id string) error
	ShouldTaskReloadItems(taskId, id string) bool
	RequireTaskReloadItems(taskId, id string) error
	ClearTaskReloadItems(taskId, id string) error

	// task assignments
	GetTaskAssignment(taskId, itemId string) (*definition.TaskAssignment, error)
	GetTaskAssignments(taskId string) ([]*definition.TaskAssignment, error)
	SetTaskAssignment(assignment *definition.TaskAssignment) error
	RemoveTaskAssignment(taskId, itemId string) error

	// strategy related
	GetStrategy(id string) (*definition.Strategy, error)
	GetStrategies() ([]*definition.Strategy, error)
	CreateStrategy(strategy *definition.Strategy) error
	UpdateStrategy(strategy *definition.Strategy) error
	DeleteStrategy(id string) error

	// strategy runtimes
	GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error)
	GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error)
	SetStrategyRuntime(runtime *definition.StrategyRuntime) error
	RemoveStrategyRuntime(strategyId, schedulerId string) error

	// Dump dump data in storage in string format.
	Dump() string
}
