package storetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/stretchr/testify/assert"
)

func DoTestName(t *testing.T, s store.Store, name string) {
	assert.Equal(t, name, s.Name())
}

func DoTestTime(t *testing.T, s store.Store) {
	t0 := s.Time()
	time.Sleep(time.Millisecond * 200)
	t1 := s.Time()
	assert.True(t, t0 > 0)
	assert.True(t, t1 > 0)
	assert.True(t, t1 > t0)
	assert.True(t, (t1-t0) > 190 && (t1-t0) < 210)
}

func DoTestSequence(t *testing.T, s store.Store) {
	s0, _ := s.Sequence()
	s1, _ := s.Sequence()
	assert.True(t, s0 > 0)
	assert.True(t, s1 > 0)
	assert.True(t, s1 > s0)
	assert.True(t, (s1-s0) == 1)
}

func DoTestTask(t *testing.T, s store.Store) {
	taskOri := &definition.Task{
		Id:            "demo-task",
		ExecutorCount: 3,
	}

	// try to fetch not existed task
	task, err := s.GetTask(taskOri.Id)
	assert.Nil(t, task)
	assert.Equal(t, store.NotExist, err)

	// try to update not existed task
	err = s.UpdateTask(taskOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)

	// create
	err = s.CreateTask(taskOri)
	assert.Nil(t, err)

	// verify creation
	task, err = s.GetTask(taskOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, taskOri.Id, task.Id)

	// recreation
	err = s.CreateTask(taskOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.AlreadyExist, err)

	// task list
	arr, err := s.GetTasks()
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, taskOri.Id, arr[0].Id)

	// modify
	taskOri.ExecutorCount = 44
	err = s.UpdateTask(taskOri)
	assert.Nil(t, err)

	// verify modify
	task, err = s.GetTask(taskOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, taskOri.Id, task.Id)
	assert.Equal(t, 44, task.ExecutorCount)

	// delete
	err = s.RemoveTask(taskOri.Id)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveTask(taskOri.Id)
	assert.Equal(t, store.NotExist, err)

	// verify delete
	task, err = s.GetTask(taskOri.Id)
	assert.Nil(t, task)
	assert.Equal(t, store.NotExist, err)
}

func DoTestStrategy(t *testing.T, s store.Store) {
	strategyOri := &definition.Strategy{
		Id:    "demo-strategy",
		Total: 3,
	}

	// try to fetch not existed task
	strategy, err := s.GetStrategy(strategyOri.Id)
	assert.Nil(t, strategy)
	assert.Equal(t, store.NotExist, err)

	// try to update not existed task
	err = s.UpdateStrategy(strategyOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)

	// create
	err = s.CreateStrategy(strategyOri)
	assert.Nil(t, err)

	// verify creation
	strategy, err = s.GetStrategy(strategyOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, strategy)
	assert.Equal(t, strategyOri.Id, strategy.Id)

	// recreation
	err = s.CreateStrategy(strategyOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.AlreadyExist, err)

	// task list
	arr, err := s.GetStrategies()
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, strategyOri.Id, arr[0].Id)

	// modify
	strategyOri.Total = 44
	err = s.UpdateStrategy(strategyOri)
	assert.Nil(t, err)

	// verify modify
	strategy, err = s.GetStrategy(strategyOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, strategy)
	assert.Equal(t, strategyOri.Id, strategy.Id)
	assert.Equal(t, 44, strategy.Total)

	// delete
	err = s.RemoveStrategy(strategyOri.Id)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategy(strategyOri.Id)
	assert.Equal(t, store.NotExist, err)

	// verify delete
	strategy, err = s.GetStrategy(strategyOri.Id)
	assert.Nil(t, strategy)
	assert.Equal(t, store.NotExist, err)
}

func DoTestStrategyRuntime(t *testing.T, s store.Store) {
	runtimeOri1 := &definition.StrategyRuntime{
		StrategyId:  "strategy1",
		SchedulerId: "scheduler1",
	}
	runtimeOri2 := &definition.StrategyRuntime{
		StrategyId:  "strategy1",
		SchedulerId: "scheduler2",
	}
	runtimeOri3 := &definition.StrategyRuntime{
		StrategyId:  "strategy1",
		SchedulerId: "scheduler3",
	}
	runtimeOri4 := &definition.StrategyRuntime{
		StrategyId:  "strategy2",
		SchedulerId: "scheduler1",
	}
	runtimeOri5 := &definition.StrategyRuntime{
		StrategyId:  "strategy2",
		SchedulerId: "scheduler2",
	}

	// try to fetch not existed runtime
	runtime, err := s.GetStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.Nil(t, runtime)
	assert.Equal(t, store.NotExist, err)

	// try to delete not existed runtime
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.Nil(t, err)

	// try to create runtime
	err = s.SetStrategyRuntime(runtimeOri1)
	assert.Nil(t, err)

	// fetch it back
	runtime, err = s.GetStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.Nil(t, err)
	assert.NotNil(t, runtime)
	assert.Equal(t, runtimeOri1.StrategyId, runtime.StrategyId)
	assert.Equal(t, runtimeOri1.SchedulerId, runtime.SchedulerId)

	// try to recreate runtime
	err = s.SetStrategyRuntime(runtimeOri1)
	assert.Nil(t, err)

	// register the rest
	s.SetStrategyRuntime(runtimeOri2)
	s.SetStrategyRuntime(runtimeOri3)
	s.SetStrategyRuntime(runtimeOri4)
	s.SetStrategyRuntime(runtimeOri5)

	// verify list
	arr, err := s.GetStrategyRuntimes(runtimeOri1.StrategyId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 3, len(arr))

	arr, err = s.GetStrategyRuntimes(runtimeOri4.StrategyId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	// delete
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.Nil(t, err)

	// verify delete
	arr, err = s.GetStrategyRuntimes(runtimeOri1.StrategyId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	runtime, err = s.GetStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)
	assert.Nil(t, runtime)
}

func DoTestTaskRuntime(t *testing.T, s store.Store) {
	runtimeOri1 := &definition.TaskRuntime{
		Id:          "r0",
		StrategyId:  "strategy1",
		TaskId:      "task1",
		SchedulerId: "scheduler1",
	}
	runtimeOri2 := &definition.TaskRuntime{
		Id:          "r1",
		StrategyId:  "strategy1",
		TaskId:      "task1",
		SchedulerId: "scheduler2",
	}
	runtimeOri3 := &definition.TaskRuntime{
		Id:          "r2",
		StrategyId:  "strategy1",
		TaskId:      "task1",
		SchedulerId: "scheduler3",
	}
	runtimeOri4 := &definition.TaskRuntime{
		Id:          "r3",
		StrategyId:  "strategy2",
		TaskId:      "task2",
		SchedulerId: "scheduler1",
	}
	runtimeOri5 := &definition.TaskRuntime{
		Id:          "r4",
		StrategyId:  "strategy2",
		TaskId:      "task2",
		SchedulerId: "scheduler2",
	}

	// try to fetch not existed runtime
	runtime, err := s.GetTaskRuntime(runtimeOri1.StrategyId, runtimeOri1.TaskId, runtimeOri1.Id)
	assert.Nil(t, runtime)
	assert.Equal(t, store.NotExist, err)

	// try to delete not existed runtime
	err = s.RemoveTaskRuntime(runtimeOri1.StrategyId, runtimeOri1.TaskId, runtimeOri1.Id)
	assert.Nil(t, err)

	// try to create runtime
	err = s.SetTaskRuntime(runtimeOri1)
	assert.Nil(t, err)

	// fetch it back
	runtime, err = s.GetTaskRuntime(runtimeOri1.StrategyId, runtimeOri1.TaskId, runtimeOri1.Id)
	assert.Nil(t, err)
	assert.NotNil(t, runtime)
	assert.Equal(t, runtimeOri1.TaskId, runtime.TaskId)
	assert.Equal(t, runtimeOri1.Id, runtime.Id)

	// try to recreate runtime
	err = s.SetTaskRuntime(runtimeOri1)
	assert.Nil(t, err)

	// register the rest
	s.SetTaskRuntime(runtimeOri2)
	s.SetTaskRuntime(runtimeOri3)
	s.SetTaskRuntime(runtimeOri4)
	s.SetTaskRuntime(runtimeOri5)

	// verify list
	arr, err := s.GetTaskRuntimes(runtimeOri1.StrategyId, runtimeOri1.TaskId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 3, len(arr))

	arr, err = s.GetTaskRuntimes(runtimeOri4.StrategyId, runtimeOri4.TaskId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	// delete
	err = s.RemoveTaskRuntime(runtimeOri1.StrategyId, runtimeOri1.TaskId, runtimeOri1.Id)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyId, runtimeOri1.SchedulerId)
	assert.Nil(t, err)

	// verify delete
	arr, err = s.GetTaskRuntimes(runtimeOri1.StrategyId, runtimeOri1.TaskId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	runtime, err = s.GetTaskRuntime(runtimeOri1.StrategyId, runtimeOri1.TaskId, runtimeOri1.Id)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)
	assert.Nil(t, runtime)
}

func DoTestTaskAssignment(t *testing.T, s store.Store) {
	assignmentOri1 := &definition.TaskAssignment{
		StrategyId: "strategy1",
		TaskId:     "task1",
		ItemId:     "a",
		RuntimeId:  "r0",
	}
	assignmentOri2 := &definition.TaskAssignment{
		StrategyId: "strategy1",
		TaskId:     "task1",
		ItemId:     "b",
		RuntimeId:  "r1",
	}
	assignmentOri3 := &definition.TaskAssignment{
		StrategyId: "strategy1",
		TaskId:     "task1",
		ItemId:     "c",
		RuntimeId:  "r0",
	}
	assignmentOri4 := &definition.TaskAssignment{
		StrategyId: "strategy1",
		TaskId:     "task2",
		ItemId:     "a",
		RuntimeId:  "r0",
	}
	assignmentOri5 := &definition.TaskAssignment{
		StrategyId: "strategy1",
		TaskId:     "task2",
		ItemId:     "b",
		RuntimeId:  "r1",
	}

	// try to fetch not existed data
	assignment, err := s.GetTaskAssignment(assignmentOri1.StrategyId, assignmentOri1.TaskId, assignmentOri1.ItemId)
	assert.Nil(t, assignment)
	assert.Equal(t, store.NotExist, err)

	// try to delete not existed data
	err = s.RemoveTaskAssignment(assignmentOri1.StrategyId, assignmentOri1.TaskId, assignmentOri1.ItemId)
	assert.Nil(t, err)

	// try to create one
	err = s.SetTaskAssignment(assignmentOri1)
	assert.Nil(t, err)

	// fetch it back
	assignment, err = s.GetTaskAssignment(assignmentOri1.StrategyId, assignmentOri1.TaskId, assignmentOri1.ItemId)
	assert.Nil(t, err)
	assert.NotNil(t, assignment)
	assert.Equal(t, assignmentOri1.TaskId, assignment.TaskId)
	assert.Equal(t, assignmentOri1.ItemId, assignment.ItemId)

	// try to recreate runtime
	err = s.SetTaskAssignment(assignmentOri1)
	assert.Nil(t, err)

	// register the rest
	s.SetTaskAssignment(assignmentOri2)
	s.SetTaskAssignment(assignmentOri3)
	s.SetTaskAssignment(assignmentOri4)
	s.SetTaskAssignment(assignmentOri5)

	// verify list
	arr, err := s.GetTaskAssignments(assignmentOri1.StrategyId, assignmentOri1.TaskId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 3, len(arr))

	arr, err = s.GetTaskAssignments(assignmentOri4.StrategyId, assignmentOri4.TaskId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	// delete
	err = s.RemoveTaskAssignment(assignmentOri1.StrategyId, assignmentOri1.TaskId, assignmentOri1.ItemId)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategyRuntime(assignmentOri1.TaskId, assignmentOri1.ItemId)
	assert.Nil(t, err)

	// verify delete
	arr, err = s.GetTaskAssignments(assignmentOri1.StrategyId, assignmentOri1.TaskId)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	assignment, err = s.GetTaskAssignment(assignmentOri1.StrategyId, assignmentOri1.TaskId, assignmentOri1.ItemId)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)
	assert.Nil(t, assignment)
}

func DoTestScheduler(t *testing.T, s store.Store) {
	schedulerOri := &definition.Scheduler{
		Id: "demo-scheduler",
	}
	list, _ := s.GetSchedulers()
	for _, scheduler := range list {
		s.UnregisterScheduler(scheduler.Id)
	}

	list, err := s.GetSchedulers()
	assert.Nil(t, err)
	assert.Empty(t, list)

	s.RegisterScheduler(schedulerOri)

	list, err = s.GetSchedulers()
	assert.Nil(t, err)
	assert.NotEmpty(t, list)

	scheduler, err := s.GetScheduler(schedulerOri.Id)
	assert.Nil(t, err)
	assert.Equal(t, schedulerOri.Id, scheduler.Id)

	scheduler, err = s.GetScheduler("not existed")
	assert.Nil(t, scheduler)
	assert.NotNil(t, err)

	s.UnregisterScheduler(schedulerOri.Id)

	list, err = s.GetSchedulers()
	assert.Nil(t, err)
	assert.Empty(t, list)
}

func DoTestTaskReloadItems(t *testing.T, s store.Store) {
	ver, err := s.GetTaskItemsConfigVersion("s0", "t0")
	assert.Nil(t, err)
	assert.True(t, ver >= 0)
	assert.Nil(t, s.IncreaseTaskItemsConfigVersion("s0", "t0"))
	ver1, err := s.GetTaskItemsConfigVersion("s0", "t0")
	assert.Nil(t, err)
	assert.True(t, ver1 > ver)
}

func DoTestDump(t *testing.T, s store.Store) {
	scheduler := &definition.Scheduler{
		Id: "demo-scheduler",
	}
	s.RegisterScheduler(scheduler)

	strategy := &definition.Strategy{
		Id: "demo-strategy",
	}
	s.CreateStrategy(strategy)

	runtime := &definition.StrategyRuntime{
		StrategyId:  "demo-strategy",
		SchedulerId: "demo-scheduler",
		Num:         93944,
	}
	s.SetStrategyRuntime(runtime)

	str := s.Dump()
	fmt.Println(str)
	assert.Contains(t, str, "demo-scheduler")
	assert.Contains(t, str, "demo-strategy")
	assert.Contains(t, str, "93944")

	s.RemoveStrategyRuntime(runtime.StrategyId, runtime.SchedulerId)
	s.RemoveStrategy(strategy.Id)
	s.UnregisterScheduler(scheduler.Id)
}
