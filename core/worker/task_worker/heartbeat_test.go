package task_worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/stretchr/testify/assert"
)

const (
	TEST_STRATEGY_ID = "s0"
	TEST_TASK_ID     = "t0"
	TEST_ITEM_ID1    = "p0"
	TEST_ITEM_ID2    = "p1"
)

var (
	memoryStore = memory.New()
)

type DemoHeartbeatTask struct {
	Name string
}

func (d *DemoHeartbeatTask) Select(parameter, ownSign string, items []definition.TaskItem, eachFetchNum int) []interface{} {
	fmt.Println("sel()")
	time.Sleep(20 * time.Millisecond)
	return []interface{}{1, 2, 3}
}

func (d *DemoHeartbeatTask) Execute(task interface{}, ownSign string) bool {
	fmt.Println("exe()")
	time.Sleep(20 * time.Millisecond)
	return true
}

func newTaskWorker() *TaskWorker {
	RegisterTaskTypeName("demoHeartbeat", &DemoHeartbeatTask{})
	manager, _ := core.New(memoryStore)
	item1 := definition.TaskItem{
		Id: TEST_ITEM_ID1,
	}
	item2 := definition.TaskItem{
		Id: TEST_ITEM_ID2,
	}
	w, _ := NewTask(definition.Strategy{
		Id:      "s0",
		IpList:  []string{"127.0.0.1"},
		Total:   1,
		Kind:    definition.TaskKind,
		Bind:    TEST_TASK_ID,
		Enabled: true,
	}, definition.Task{
		Id:                TEST_TASK_ID,
		Bind:              "demoHeartbeat",
		BatchCount:        1,
		ExecutorCount:     1,
		HeartbeatInterval: 200,
		DeathTimeout:      30000,
		Items: []definition.TaskItem{
			item1,
			item2,
		},
	}, memoryStore, manager.Scheduler().Id)
	return w.(*TaskWorker)
}

func clearStore() {
	runtimes, _ := memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	for _, r := range runtimes {
		memoryStore.RemoveTaskRuntime(r.StrategyId, r.TaskId, r.Id)
	}

	assignments, _ := memoryStore.GetTaskAssignments(TEST_STRATEGY_ID, TEST_TASK_ID)
	for _, t := range assignments {
		memoryStore.RemoveTaskAssignment(t.StrategyId, t.TaskId, t.ItemId)
	}
}

func TestRegisterTaskRuntime(t *testing.T) {
	clearStore()
	w := newTaskWorker()
	runtimes1, _ := memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	w.registerTaskRuntime()
	runtimes2, _ := memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.Equal(t, 1, len(runtimes2)-len(runtimes1))
	ver1 := runtimes2[len(runtimes2)-1].Version
	w.registerTaskRuntime()
	runtimes3, _ := memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	ver2 := runtimes3[len(runtimes3)-1].Version
	assert.Equal(t, len(runtimes2), len(runtimes3))
	assert.Equal(t, int64(1), ver2-ver1)
}

func TestHeartBeat(t *testing.T) {
	clearStore()
	w := newTaskWorker()
	go w.heartbeat()
	time.Sleep(500 * time.Millisecond)
	runtimes1, _ := memoryStore.GetTaskRuntimes(TEST_STRATEGY_ID, TEST_TASK_ID)
	assert.True(t, len(runtimes1) > 0)
	w.needStop = true
	select {
	case val := <-w.notifierC:
		assert.Equal(t, 2, val)
	case <-time.After(500 * time.Millisecond):
		assert.Fail(t, "Fail to stop")
	}
}
