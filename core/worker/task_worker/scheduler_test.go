// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/stretchr/testify/assert"
)

func TestClearExpiredRuntimes(t *testing.T) {
	clearStore()
	now := time.Now().Unix() * 1000
	validRuntime := &definition.TaskRuntime{
		ID:            "r1",
		LastHeartbeat: now,
		TaskID:        TEST_TASK_ID,
		StrategyID:    TEST_STRATEGY_ID,
	}
	memoryStore.SetTaskRuntime(validRuntime)
	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		ID:            "r0",
		LastHeartbeat: now - 3600*1000,
		TaskID:        TEST_TASK_ID,
		StrategyID:    TEST_STRATEGY_ID,
	})
	runtimes, _ := memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 2, len(runtimes))
	w := newTaskWorker()
	uuids, validRuntimes, err := w.clearExpiredRuntimes()
	assert.Nil(t, err)
	runtimes, _ = memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 1, len(runtimes))
	assert.Equal(t, validRuntime.ID, runtimes[0].ID)

	assert.Equal(t, 1, len(uuids))
	assert.Equal(t, 1, len(validRuntimes))
	assert.Equal(t, validRuntime.ID, uuids[0])
	assert.Equal(t, validRuntime.ID, validRuntimes[0].ID)
}

func TestGetCurrentAssignments(t *testing.T) {
	clearStore()
	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		ID:            "r1",
		LastHeartbeat: time.Now().Unix() * 1000,
		TaskID:        TEST_TASK_ID,
		StrategyID:    TEST_STRATEGY_ID,
	})
	w := newTaskWorker()
	w.registerTaskRuntime()
	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		StrategyID: TEST_STRATEGY_ID,
		TaskID:     TEST_TASK_ID,
		ItemID:     "p1",
		RuntimeID:  w.runtime.ID,
	})
	assignments1, _ := memoryStore.GetTaskAssignments(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 1, len(assignments1))

	assignMap, spares, runtimeAssigns, _ := w.getCurrentAssignments()

	assignments2, _ := memoryStore.GetTaskAssignments(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 2, len(assignments2))

	assert.NotNil(t, assignMap)
	assert.NotNil(t, spares)
	assert.NotNil(t, runtimeAssigns)
	assert.Equal(t, 2, len(assignMap))
	assert.Equal(t, 1, len(spares))
	assert.Equal(t, 2, len(runtimeAssigns))
	assert.True(t, len(runtimeAssigns[0].Items) > len(runtimeAssigns[1].Items))

	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		StrategyID:         TEST_STRATEGY_ID,
		TaskID:             TEST_TASK_ID,
		ItemID:             "p0",
		RuntimeID:          w.runtime.ID,
		RequestedRuntimeID: "r1",
	})
	_, _, runtimeAssigns, _ = w.getCurrentAssignments()
	assert.Equal(t, len(runtimeAssigns[0].Items), len(runtimeAssigns[1].Items))

	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		StrategyID:         TEST_STRATEGY_ID,
		TaskID:             TEST_TASK_ID,
		ItemID:             "p0",
		RuntimeID:          w.runtime.ID,
		RequestedRuntimeID: RUNTIME_EMPTY,
	})
	_, _, runtimeAssigns, _ = w.getCurrentAssignments()
	assert.True(t, len(runtimeAssigns[0].Items) > len(runtimeAssigns[1].Items))

	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		StrategyID:         TEST_STRATEGY_ID,
		TaskID:             TEST_TASK_ID,
		ItemID:             "p0",
		RuntimeID:          "",
		RequestedRuntimeID: RUNTIME_EMPTY,
	})
	_, _, runtimeAssigns, _ = w.getCurrentAssignments()
	assert.True(t, len(runtimeAssigns[0].Items) > len(runtimeAssigns[1].Items))
}

func TestDistributeTaskItems(t *testing.T) {
	clearStore()
	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		ID:            "r1$11111111111111",
		LastHeartbeat: time.Now().Unix() * 1000,
		TaskID:        TEST_TASK_ID,
		StrategyID:    TEST_STRATEGY_ID,
	})
	w := newTaskWorker()
	w.registerTaskRuntime()

	assignments, _ := memoryStore.GetTaskAssignments(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 0, len(assignments))

	ver, _ := memoryStore.GetTaskItemsConfigVersion(w.strategyDefine.ID, TEST_TASK_ID)
	w.distributeTaskItems()

	assignments, _ = memoryStore.GetTaskAssignments(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 2, len(assignments))
	for _, assign := range assignments {
		assert.NotEmpty(t, assign.RuntimeID)
		assert.Empty(t, assign.RequestedRuntimeID)
	}
	ver1, _ := memoryStore.GetTaskItemsConfigVersion(w.strategyDefine.ID, TEST_TASK_ID)
	assert.True(t, ver1 > ver)
}

func TestReloadTaskItems(t *testing.T) {
	clearStore()
	w := newTaskWorker()
	assert.Equal(t, 0, len(w.taskItems))

	w.registerTaskRuntime()
	w.distributeTaskItems()
	w.reloadTaskItems()

	assert.Equal(t, 2, len(w.taskItems))

	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		ID: "r1",
	})
	assign, _ := memoryStore.GetTaskAssignment(TEST_STRATEGY_ID, TEST_TASK_ID, TEST_ITEM_ID1)
	assign.RequestedRuntimeID = "r1"
	memoryStore.SetTaskAssignment(assign)
	ver, _ := memoryStore.GetTaskItemsConfigVersion(w.strategyDefine.ID, TEST_TASK_ID)

	w.reloadTaskItems()

	assert.Equal(t, 1, len(w.taskItems))
	ver1, _ := memoryStore.GetTaskItemsConfigVersion(w.strategyDefine.ID, TEST_TASK_ID)
	assert.True(t, ver1 > ver)
}

func TestSchedule(t *testing.T) {
	clearStore()
	w := newTaskWorker()
	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		StrategyID: TEST_STRATEGY_ID,
		TaskID:     TEST_TASK_ID,
		ItemID:     TEST_ITEM_ID1,
		RuntimeID:  w.runtime.ID,
	})
	w.distributeTaskItems()

	r, _ := memoryStore.GetTaskAssignment(TEST_STRATEGY_ID, TEST_TASK_ID, TEST_ITEM_ID1)
	assert.NotNil(t, r)
	assert.Equal(t, w.runtime.ID, r.RuntimeID)

	w.cleanupSchedule()
	r, _ = memoryStore.GetTaskAssignment(TEST_STRATEGY_ID, TEST_TASK_ID, TEST_ITEM_ID1)
	assert.NotNil(t, r)
	assert.NotEqual(t, w.runtime.ID, r.RuntimeID)
}
