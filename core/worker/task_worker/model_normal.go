// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/jasonjoo2010/goschedule/log"
	"github.com/jasonjoo2010/goschedule/utils"
)

// select() -> execute() -> execute() [queue empty && all done] -> select()

type NormalModel struct {
	mu       sync.Mutex
	notifier chan int

	ctx       context.Context
	ctxCancel context.CancelFunc

	worker  *TaskWorker
	waiting int32
}

func NewNormalModel(worker *TaskWorker) *NormalModel {
	m := &NormalModel{
		worker:   worker,
		notifier: make(chan int),
	}

	m.ctx, m.ctxCancel = context.WithCancel(context.Background())
	return m
}

func (m *NormalModel) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ctxCancel()
	close(m.notifier)
}

func (m *NormalModel) notifyAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utils.ContextDone(m.ctx) {
		return
	}

	cnt := atomic.LoadInt32(&m.waiting)
	for i := 0; i < int(cnt); i++ {
		select {
		case m.notifier <- 1:
		default:
			log.Warn("No waiting executor can be notified")
			return
		}
	}
}

func (m *NormalModel) LoopOnce() {
	if m.worker.executeOnceOrReturn() {
		return
	}

	if utils.ContextDone(m.ctx) {
		return
	}

	// when queue empty
	cur := int(atomic.AddInt32(&m.waiting, 1))
	if cur == m.worker.taskDefine.ExecutorCount {
		// Only last one can fetch new data
		// Release first from waiting
		atomic.AddInt32(&m.waiting, -1)
		m.worker.selectOnce()
		m.notifyAll()
	} else {
		// block to be notified
		defer atomic.AddInt32(&m.waiting, -1)
		<-m.notifier
	}
}
