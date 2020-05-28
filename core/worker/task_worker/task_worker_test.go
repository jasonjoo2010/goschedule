package task_worker

import (
	"reflect"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/stretchr/testify/assert"
)

func TestGeneral(t *testing.T) {
	w := newTaskWorker()
	w.Start("s0", "")
	time.Sleep(100 * time.Millisecond)
	assert.True(t, w.started)
	w.Stop("s0", "")
	assert.False(t, w.started)
}

func TestSelectOnce(t *testing.T) {
	w := newTaskWorker()
	memoryStore.RequireTaskReloadItems(w.taskDefine.Id, w.runtime.Id)
	assert.Equal(t, 0, len(w.data))
	w.selectOnce()
	assert.Equal(t, 3, len(w.data))
}

func TestRegister(t *testing.T) {
	RegisterTaskType(&DemoHeartbeatTask{})
	RegisterTaskType(&DemoHeartbeatTask{})
	assert.NotNil(t, getTask(utils.TypeName(DemoHeartbeatTask{})))
	RegisterTaskTypeName("a", &DemoHeartbeatTask{})
	assert.Equal(t, reflect.TypeOf(&DemoHeartbeatTask{}), reflect.TypeOf(getTask("a")))

	heartbeatTask := getTask("a")
	assert.NotNil(t, heartbeatTask)
	assert.Equal(t, 3, len(heartbeatTask.Select("asdf", "", []definition.TaskItem{}, 10)))

	inst := &DemoHeartbeatTask{
		Name: "i0",
	}
	RegisterTaskInst(inst)
	RegisterTaskInstName("b", inst)
	assert.Equal(t, inst, getTask(utils.TypeName(inst)))
	assert.Equal(t, inst, getTask("b"))

	var demoTask *DemoHeartbeatTask
	var ok bool
	demoTask, ok = getTask("b").(*DemoHeartbeatTask)
	assert.True(t, ok)
	assert.Equal(t, "i0", demoTask.Name)
}