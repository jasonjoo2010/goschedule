// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"sync"
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
	demo := &demoTaskBatch{}
	batch := BatchExecutor{
		worker: &TaskWorker{
			data: make(chan interface{}, 100),
			taskDefine: definition.Task{
				BatchCount: 6,
			},
		},
		task: demo,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]interface{}, 0, 6)
			},
		},
	}
	batch.worker.data <- 1
	batch.worker.data <- 2
	batch.worker.data <- 3
	batch.worker.data <- 4
	batch.worker.data <- 5
	batch.worker.data <- 6
	batch.worker.data <- 7
	batch.worker.data <- 8
	batch.worker.data <- 9
	batch.worker.data <- 10
	demo.succ = true
	batch.ExecuteOrReturn()
	demo.succ = false
	assert.True(t, batch.ExecuteOrReturn())
	assert.False(t, batch.ExecuteOrReturn())
	assert.False(t, batch.ExecuteOrReturn())
	assert.False(t, batch.ExecuteOrReturn())

	assert.Equal(t, int64(1), batch.worker.Statistics.ExecuteSuccCount)
	assert.Equal(t, int64(1), batch.worker.Statistics.ExecuteFailCount)
}
