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
	single.worker.data <- 5
	demo.succ = true
	single.ExecuteOrReturn()
	single.ExecuteOrReturn()
	single.ExecuteOrReturn()
	demo.succ = false
	single.ExecuteOrReturn()
	assert.True(t, single.ExecuteOrReturn())
	assert.False(t, single.ExecuteOrReturn())
	assert.False(t, single.ExecuteOrReturn())

	assert.Equal(t, int64(3), single.worker.Statistics.ExecuteSuccCount)
	assert.Equal(t, int64(2), single.worker.Statistics.ExecuteFailCount)
}
