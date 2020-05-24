package task_worker

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/stretchr/testify/assert"
)

func TestClearExpiredRuntimes(t *testing.T) {
	clearStore()
	now := time.Now().Unix() * 1000
	validRuntime := &definition.TaskRuntime{
		Id:            "r1",
		LastHeartBeat: now,
		TaskId:        TEST_TASK_ID,
	}
	memoryStore.SetTaskRuntime(validRuntime)
	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		Id:            "r0",
		LastHeartBeat: now - 3600*1000,
		TaskId:        TEST_TASK_ID,
	})
	runtimes, _ := memoryStore.GetTaskRuntimes(TEST_TASK_ID)
	assert.Equal(t, 2, len(runtimes))
	w := newTaskWorker()
	uuids, validRuntimes, err := w.clearExpiredRuntimes()
	assert.Nil(t, err)
	runtimes, _ = memoryStore.GetTaskRuntimes(TEST_TASK_ID)
	assert.Equal(t, 1, len(runtimes))
	assert.Equal(t, validRuntime.Id, runtimes[0].Id)

	assert.Equal(t, 1, len(uuids))
	assert.Equal(t, 1, len(validRuntimes))
	assert.Equal(t, validRuntime.Id, uuids[0])
	assert.Equal(t, validRuntime.Id, validRuntimes[0].Id)
}

func TestGetCurrentAssignments(t *testing.T) {
	clearStore()
	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		Id:            "r1",
		LastHeartBeat: time.Now().Unix() * 1000,
		TaskId:        TEST_TASK_ID,
	})
	w := newTaskWorker()
	w.registerTaskRuntime()
	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		TaskId:    TEST_TASK_ID,
		ItemId:    "p1",
		RuntimeId: w.runtime.Id,
	})
	assignments1, _ := memoryStore.GetTaskAssignments(TEST_TASK_ID)
	assert.Equal(t, 1, len(assignments1))

	assignMap, spares, runtimeAssigns, _ := w.getCurrentAssignments()

	assignments2, _ := memoryStore.GetTaskAssignments(TEST_TASK_ID)
	assert.Equal(t, 2, len(assignments2))

	assert.NotNil(t, assignMap)
	assert.NotNil(t, spares)
	assert.NotNil(t, runtimeAssigns)
	assert.Equal(t, 2, len(assignMap))
	assert.Equal(t, 1, len(spares))
	assert.Equal(t, 2, len(runtimeAssigns))
	assert.True(t, len(runtimeAssigns[0].Items) > len(runtimeAssigns[1].Items))
}

func TestDistributeTaskItems(t *testing.T) {
	clearStore()
	memoryStore.SetTaskRuntime(&definition.TaskRuntime{
		Id:            "r1$11111111111111",
		LastHeartBeat: time.Now().Unix() * 1000,
		TaskId:        TEST_TASK_ID,
	})
	w := newTaskWorker()
	w.registerTaskRuntime()

	assignments, _ := memoryStore.GetTaskAssignments(TEST_TASK_ID)
	assert.Equal(t, 0, len(assignments))

	w.distributeTaskItems()

	assignments, _ = memoryStore.GetTaskAssignments(TEST_TASK_ID)
	assert.Equal(t, 2, len(assignments))
	for _, assign := range assignments {
		assert.NotEmpty(t, assign.RuntimeId)
		assert.Empty(t, assign.RequestedRuntimeId)
	}
	assert.True(t, memoryStore.ShouldTaskReloadItems(TEST_TASK_ID, w.runtime.Id))
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
		Id: "r1",
	})
	assign, _ := memoryStore.GetTaskAssignment(TEST_TASK_ID, TEST_ITEM_ID1)
	assign.RequestedRuntimeId = "r1"
	memoryStore.SetTaskAssignment(assign)

	w.reloadTaskItems()

	assert.Equal(t, 1, len(w.taskItems))
	assert.False(t, memoryStore.ShouldTaskReloadItems(TEST_TASK_ID, w.runtime.Id))
	assert.True(t, memoryStore.ShouldTaskReloadItems(TEST_TASK_ID, "r1"))
}

func TestSchedule(t *testing.T) {
	clearStore()
	w := newTaskWorker()
	memoryStore.SetTaskAssignment(&definition.TaskAssignment{
		TaskId:    TEST_TASK_ID,
		ItemId:    TEST_ITEM_ID1,
		RuntimeId: w.runtime.Id,
	})
	go w.schedule()

	r, _ := memoryStore.GetTaskAssignment(TEST_TASK_ID, TEST_ITEM_ID1)
	assert.NotNil(t, r)
	assert.Equal(t, w.runtime.Id, r.RuntimeId)

	w.needStop = true
	select {
	case val := <-w.notifier:
		assert.Equal(t, 4, val)
	case <-time.After(500 * time.Millisecond):
		assert.Fail(t, "Can not stop schedule")
	}

	r, _ = memoryStore.GetTaskAssignment(TEST_TASK_ID, TEST_ITEM_ID1)
	assert.NotNil(t, r)
	assert.NotEqual(t, w.runtime.Id, r.RuntimeId)
}
