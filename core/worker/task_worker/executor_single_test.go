package task_worker

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/stretchr/testify/assert"
)

type demoTaskSingle struct {
	succ bool
}

func (demo *demoTaskSingle) Select(parameter, ownSign string, items []definition.TaskItem, eachFetchNum int) []interface{} {
	return make([]interface{}, 0)
}

func (demo *demoTaskSingle) Execute(task interface{}, ownSign string) bool {
	time.Sleep(30 * time.Millisecond)
	return demo.succ
}

func TestExecutorSingle(t *testing.T) {
	demo := &demoTaskSingle{}
	single := SingleExecutor{
		worker: &TaskWorker{
			data: make(chan interface{}, 100),
		},
		task: demo,
	}
	single.worker.data <- 1
	single.worker.data <- 2
	single.worker.data <- 3
	single.worker.data <- 4
	demo.succ = true
	single.ExecuteAndWaitWhenEmpty()
	single.ExecuteAndWaitWhenEmpty()
	single.ExecuteAndWaitWhenEmpty()
	demo.succ = false
	single.ExecuteAndWaitWhenEmpty()

	assert.Equal(t, int64(3), single.worker.Statistics.ExecuteSuccCount)
	assert.Equal(t, int64(1), single.worker.Statistics.ExecuteFailCount)
}
