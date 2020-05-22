package task_worker

import (
	"reflect"
	"testing"

	"github.com/jasonjoo2010/goschedule/core"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/stretchr/testify/assert"
)

func TestTaskWorker(t *testing.T) {
	store := memory.New()
	manager, _ := core.New(store)
	strategy := definition.Strategy{}
	task := definition.Task{}
	worker, _ := NewTask(strategy, task, false, manager)
	assert.NotNil(t, worker)

	worker.Start("s0", "")

	worker.Stop("s0", "")
}

func TestRegister(t *testing.T) {
	RegisterTaskType(&DemoHeartbeatTask{})
	RegisterTaskType(&DemoHeartbeatTask{})
	assert.NotNil(t, GetTaskType(utils.TypeName(&DemoHeartbeatTask{})))
	RegisterTaskTypeName("a", &DemoHeartbeatTask{})
	assert.NotEqual(t, reflect.TypeOf(DemoHeartbeatTask{}), GetTaskType("a"))
	assert.Equal(t, reflect.TypeOf(&DemoHeartbeatTask{}), GetTaskType("a"))

	inst := &DemoHeartbeatTask{}
	RegisterTaskInst(inst)
	RegisterTaskInstName("b", inst)
	assert.Equal(t, inst, GetTaskInst(utils.TypeName(inst)))
	assert.Equal(t, inst, GetTaskInst("b"))
}
