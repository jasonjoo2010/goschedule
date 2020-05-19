package task_worker

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/stretchr/testify/assert"
)

type demoTaskBatch struct {
	succ bool
}

func (demo *demoTaskBatch) Select(parameter, ownSign string, items []definition.TaskItem, eachFetchNum int) []interface{} {
	return make([]interface{}, 0)
}

func (demo *demoTaskBatch) Execute(task []interface{}, ownSign string) bool {
	time.Sleep(30 * time.Millisecond)
	return demo.succ
}

func TestExecutorBatch(t *testing.T) {
	demo := &demoTaskSingle{}
	single := SingleExecutor{
		worker: &TaskWorker{
			data: make(chan interface{}, 100),
			taskDefine: definition.Task{
				BatchCount: 6,
			},
		},
		task: demo,
	}
	single.worker.data <- 1
	single.worker.data <- 2
	single.worker.data <- 3
	single.worker.data <- 4
	single.worker.data <- 5
	single.worker.data <- 6
	single.worker.data <- 7
	single.worker.data <- 8
	single.worker.data <- 9
	single.worker.data <- 10
	demo.succ = true
	single.ExecuteAndWaitWhenEmpty()
	demo.succ = false
	single.ExecuteAndWaitWhenEmpty()

	assert.Equal(t, int64(1), single.worker.Statistics.ExecuteSuccCount)
	assert.Equal(t, int64(1), single.worker.Statistics.ExecuteFailCount)
}
