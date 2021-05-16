// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package storetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/definition"
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
	assert.True(t, (s1-s0) >= 1)
}

func DoTestTask(t *testing.T, s store.Store) {
	taskOri := &definition.Task{
		ID:            "demo-task",
		ExecutorCount: 3,
	}

	// try to fetch not existed task
	task, err := s.GetTask(taskOri.ID)
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
	task, err = s.GetTask(taskOri.ID)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, taskOri.ID, task.ID)

	// recreation
	err = s.CreateTask(taskOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.AlreadyExist, err)

	// task list
	arr, err := s.GetTasks()
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, taskOri.ID, arr[0].ID)

	// modify
	taskOri.ExecutorCount = 44
	err = s.UpdateTask(taskOri)
	assert.Nil(t, err)

	// verify modify
	task, err = s.GetTask(taskOri.ID)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, taskOri.ID, task.ID)
	assert.Equal(t, 44, task.ExecutorCount)

	// delete
	err = s.RemoveTask(taskOri.ID)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveTask(taskOri.ID)
	assert.Equal(t, store.NotExist, err)

	// verify delete
	task, err = s.GetTask(taskOri.ID)
	assert.Nil(t, task)
	assert.Equal(t, store.NotExist, err)
}

func DoTestStrategy(t *testing.T, s store.Store) {
	strategyOri := &definition.Strategy{
		ID:    "demo-strategy",
		Total: 3,
	}

	// try to fetch not existed task
	strategy, err := s.GetStrategy(strategyOri.ID)
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
	strategy, err = s.GetStrategy(strategyOri.ID)
	assert.Nil(t, err)
	assert.NotNil(t, strategy)
	assert.Equal(t, strategyOri.ID, strategy.ID)

	// recreation
	err = s.CreateStrategy(strategyOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.AlreadyExist, err)

	// task list
	arr, err := s.GetStrategies()
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, strategyOri.ID, arr[0].ID)

	// modify
	strategyOri.Total = 44
	err = s.UpdateStrategy(strategyOri)
	assert.Nil(t, err)

	// verify modify
	strategy, err = s.GetStrategy(strategyOri.ID)
	assert.Nil(t, err)
	assert.NotNil(t, strategy)
	assert.Equal(t, strategyOri.ID, strategy.ID)
	assert.Equal(t, 44, strategy.Total)

	// delete
	err = s.RemoveStrategy(strategyOri.ID)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategy(strategyOri.ID)
	assert.Equal(t, store.NotExist, err)

	// verify delete
	strategy, err = s.GetStrategy(strategyOri.ID)
	assert.Nil(t, strategy)
	assert.Equal(t, store.NotExist, err)
}

func DoTestStrategyRuntime(t *testing.T, s store.Store) {
	runtimeOri1 := &definition.StrategyRuntime{
		StrategyID:  "strategy1",
		SchedulerID: "scheduler1",
	}
	runtimeOri2 := &definition.StrategyRuntime{
		StrategyID:  "strategy1",
		SchedulerID: "scheduler2",
	}
	runtimeOri3 := &definition.StrategyRuntime{
		StrategyID:  "strategy1",
		SchedulerID: "scheduler3",
	}
	runtimeOri4 := &definition.StrategyRuntime{
		StrategyID:  "strategy2",
		SchedulerID: "scheduler1",
	}
	runtimeOri5 := &definition.StrategyRuntime{
		StrategyID:  "strategy2",
		SchedulerID: "scheduler2",
	}

	// try to fetch not existed runtime
	runtime, err := s.GetStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.Nil(t, runtime)
	assert.Equal(t, store.NotExist, err)

	// try to delete not existed runtime
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.Nil(t, err)

	// try to create runtime
	err = s.SetStrategyRuntime(runtimeOri1)
	assert.Nil(t, err)

	// fetch it back
	runtime, err = s.GetStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.Nil(t, err)
	assert.NotNil(t, runtime)
	assert.Equal(t, runtimeOri1.StrategyID, runtime.StrategyID)
	assert.Equal(t, runtimeOri1.SchedulerID, runtime.SchedulerID)

	// try to recreate runtime
	err = s.SetStrategyRuntime(runtimeOri1)
	assert.Nil(t, err)

	// register the rest
	s.SetStrategyRuntime(runtimeOri2)
	s.SetStrategyRuntime(runtimeOri3)
	s.SetStrategyRuntime(runtimeOri4)
	s.SetStrategyRuntime(runtimeOri5)

	// verify list
	arr, err := s.GetStrategyRuntimes(runtimeOri1.StrategyID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 3, len(arr))

	arr, err = s.GetStrategyRuntimes(runtimeOri4.StrategyID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	// delete
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.Nil(t, err)

	// verify delete
	arr, err = s.GetStrategyRuntimes(runtimeOri1.StrategyID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	runtime, err = s.GetStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)
	assert.Nil(t, runtime)
}

func DoTestTaskRuntime(t *testing.T, s store.Store) {
	runtimeOri1 := &definition.TaskRuntime{
		ID:          "r0",
		StrategyID:  "strategy1",
		TaskID:      "task1",
		SchedulerID: "scheduler1",
	}
	runtimeOri2 := &definition.TaskRuntime{
		ID:          "r1",
		StrategyID:  "strategy1",
		TaskID:      "task1",
		SchedulerID: "scheduler2",
	}
	runtimeOri3 := &definition.TaskRuntime{
		ID:          "r2",
		StrategyID:  "strategy1",
		TaskID:      "task1",
		SchedulerID: "scheduler3",
	}
	runtimeOri4 := &definition.TaskRuntime{
		ID:          "r3",
		StrategyID:  "strategy2",
		TaskID:      "task2",
		SchedulerID: "scheduler1",
	}
	runtimeOri5 := &definition.TaskRuntime{
		ID:          "r4",
		StrategyID:  "strategy2",
		TaskID:      "task2",
		SchedulerID: "scheduler2",
	}

	// try to fetch not existed runtime
	runtime, err := s.GetTaskRuntime(runtimeOri1.StrategyID, runtimeOri1.TaskID, runtimeOri1.ID)
	assert.Nil(t, runtime)
	assert.Equal(t, store.NotExist, err)

	// try to delete not existed runtime
	err = s.RemoveTaskRuntime(runtimeOri1.StrategyID, runtimeOri1.TaskID, runtimeOri1.ID)
	assert.Nil(t, err)

	// try to create runtime
	err = s.SetTaskRuntime(runtimeOri1)
	assert.Nil(t, err)

	// fetch it back
	runtime, err = s.GetTaskRuntime(runtimeOri1.StrategyID, runtimeOri1.TaskID, runtimeOri1.ID)
	assert.Nil(t, err)
	assert.NotNil(t, runtime)
	assert.Equal(t, runtimeOri1.TaskID, runtime.TaskID)
	assert.Equal(t, runtimeOri1.ID, runtime.ID)

	// try to recreate runtime
	err = s.SetTaskRuntime(runtimeOri1)
	assert.Nil(t, err)

	// register the rest
	s.SetTaskRuntime(runtimeOri2)
	s.SetTaskRuntime(runtimeOri3)
	s.SetTaskRuntime(runtimeOri4)
	s.SetTaskRuntime(runtimeOri5)

	// verify list
	arr, err := s.GetTaskRuntimes(runtimeOri1.StrategyID, runtimeOri1.TaskID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 3, len(arr))

	arr, err = s.GetTaskRuntimes(runtimeOri4.StrategyID, runtimeOri4.TaskID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	// delete
	err = s.RemoveTaskRuntime(runtimeOri1.StrategyID, runtimeOri1.TaskID, runtimeOri1.ID)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategyRuntime(runtimeOri1.StrategyID, runtimeOri1.SchedulerID)
	assert.Nil(t, err)

	// verify delete
	arr, err = s.GetTaskRuntimes(runtimeOri1.StrategyID, runtimeOri1.TaskID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	runtime, err = s.GetTaskRuntime(runtimeOri1.StrategyID, runtimeOri1.TaskID, runtimeOri1.ID)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)
	assert.Nil(t, runtime)
}

func DoTestTaskAssignment(t *testing.T, s store.Store) {
	assignmentOri1 := &definition.TaskAssignment{
		StrategyID: "strategy1",
		TaskID:     "task1",
		ItemID:     "a",
		RuntimeID:  "r0",
	}
	assignmentOri2 := &definition.TaskAssignment{
		StrategyID: "strategy1",
		TaskID:     "task1",
		ItemID:     "b",
		RuntimeID:  "r1",
	}
	assignmentOri3 := &definition.TaskAssignment{
		StrategyID: "strategy1",
		TaskID:     "task1",
		ItemID:     "c",
		RuntimeID:  "r0",
	}
	assignmentOri4 := &definition.TaskAssignment{
		StrategyID: "strategy1",
		TaskID:     "task2",
		ItemID:     "a",
		RuntimeID:  "r0",
	}
	assignmentOri5 := &definition.TaskAssignment{
		StrategyID: "strategy1",
		TaskID:     "task2",
		ItemID:     "b",
		RuntimeID:  "r1",
	}

	// try to fetch not existed data
	assignment, err := s.GetTaskAssignment(assignmentOri1.StrategyID, assignmentOri1.TaskID, assignmentOri1.ItemID)
	assert.Nil(t, assignment)
	assert.Equal(t, store.NotExist, err)

	// try to delete not existed data
	err = s.RemoveTaskAssignment(assignmentOri1.StrategyID, assignmentOri1.TaskID, assignmentOri1.ItemID)
	assert.Nil(t, err)

	// try to create one
	err = s.SetTaskAssignment(assignmentOri1)
	assert.Nil(t, err)

	// fetch it back
	assignment, err = s.GetTaskAssignment(assignmentOri1.StrategyID, assignmentOri1.TaskID, assignmentOri1.ItemID)
	assert.Nil(t, err)
	assert.NotNil(t, assignment)
	assert.Equal(t, assignmentOri1.TaskID, assignment.TaskID)
	assert.Equal(t, assignmentOri1.ItemID, assignment.ItemID)

	// try to recreate runtime
	err = s.SetTaskAssignment(assignmentOri1)
	assert.Nil(t, err)

	// register the rest
	s.SetTaskAssignment(assignmentOri2)
	s.SetTaskAssignment(assignmentOri3)
	s.SetTaskAssignment(assignmentOri4)
	s.SetTaskAssignment(assignmentOri5)

	// verify list
	arr, err := s.GetTaskAssignments(assignmentOri1.StrategyID, assignmentOri1.TaskID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 3, len(arr))

	arr, err = s.GetTaskAssignments(assignmentOri4.StrategyID, assignmentOri4.TaskID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	// delete
	err = s.RemoveTaskAssignment(assignmentOri1.StrategyID, assignmentOri1.TaskID, assignmentOri1.ItemID)
	assert.Nil(t, err)

	// re-delete
	err = s.RemoveStrategyRuntime(assignmentOri1.TaskID, assignmentOri1.ItemID)
	assert.Nil(t, err)

	// verify delete
	arr, err = s.GetTaskAssignments(assignmentOri1.StrategyID, assignmentOri1.TaskID)
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 2, len(arr))

	assignment, err = s.GetTaskAssignment(assignmentOri1.StrategyID, assignmentOri1.TaskID, assignmentOri1.ItemID)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)
	assert.Nil(t, assignment)
}

func DoTestScheduler(t *testing.T, s store.Store) {
	schedulerOri := &definition.Scheduler{
		ID: "demo-scheduler",
	}
	list, _ := s.GetSchedulers()
	for _, scheduler := range list {
		s.UnregisterScheduler(scheduler.ID)
	}

	list, err := s.GetSchedulers()
	assert.Nil(t, err)
	assert.Empty(t, list)

	s.RegisterScheduler(schedulerOri)

	list, err = s.GetSchedulers()
	assert.Nil(t, err)
	assert.NotEmpty(t, list)

	scheduler, err := s.GetScheduler(schedulerOri.ID)
	assert.Nil(t, err)
	assert.Equal(t, schedulerOri.ID, scheduler.ID)

	scheduler, err = s.GetScheduler("not existed")
	assert.Nil(t, scheduler)
	assert.NotNil(t, err)

	s.UnregisterScheduler(schedulerOri.ID)

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
		ID: "demo-scheduler",
	}
	s.RegisterScheduler(scheduler)

	strategy := &definition.Strategy{
		ID: "demo-strategy",
	}
	s.CreateStrategy(strategy)

	runtime := &definition.StrategyRuntime{
		StrategyID:  "demo-strategy",
		SchedulerID: "demo-scheduler",
		Num:         93944,
	}
	s.SetStrategyRuntime(runtime)

	str := s.Dump()
	fmt.Println(str)
	assert.Contains(t, str, "demo-scheduler")
	assert.Contains(t, str, "demo-strategy")
	assert.Contains(t, str, "93944")

	s.RemoveStrategyRuntime(runtime.StrategyID, runtime.SchedulerID)
	s.RemoveStrategy(strategy.ID)
	s.UnregisterScheduler(scheduler.ID)
}
