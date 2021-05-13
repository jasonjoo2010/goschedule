// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"context"
	"sync"

	"github.com/jasonjoo2010/goschedule/utils"
	"golang.org/x/sync/semaphore"
)

// select() -> execute() -> execute() [queue empty] -> select() -> execute()

type StreamModel struct {
	mu sync.Mutex

	ctx       context.Context
	ctxCancel context.CancelFunc

	worker   *TaskWorker
	sem      *semaphore.Weighted
	notifier chan int
}

func NewStreamModel(worker *TaskWorker) *StreamModel {
	m := &StreamModel{
		sem:      semaphore.NewWeighted(1),
		notifier: make(chan int),
		worker:   worker,
	}

	m.ctx, m.ctxCancel = context.WithCancel(context.Background())
	return m
}

func (m *StreamModel) notifyAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utils.ContextDone(m.ctx) {
		return
	}

	close(m.notifier)
	m.notifier = make(chan int)
}

func (m *StreamModel) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ctxCancel()
	close(m.notifier)
}

func (m *StreamModel) getNotifier() <-chan int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.notifier
}

func (m *StreamModel) LoopOnce() {
	if m.worker.executeOnceOrReturn() {
		return
	}

	if utils.ContextDone(m.ctx) {
		return
	}

	if m.sem.TryAcquire(1) {
		defer m.sem.Release(1)
		m.worker.selectOnce()
		m.notifyAll()
		return
	}

	<-m.getNotifier()
}
