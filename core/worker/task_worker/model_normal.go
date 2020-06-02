// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// select() -> execute() -> execute() [queue empty && all done] -> select()

type NormalModel struct {
	sync.Mutex
	worker   *TaskWorker
	waiting  int32
	notifier chan int
	stopped  bool
}

func NewNormalModel(worker *TaskWorker) *NormalModel {
	return &NormalModel{
		worker:   worker,
		notifier: make(chan int),
	}
}

func (m *NormalModel) Stop() {
	m.Lock()
	defer m.Unlock()
	m.stopped = true
	close(m.notifier)
}

func (m *NormalModel) notifyAll() {
	m.Lock()
	defer m.Unlock()
	if m.stopped {
		return
	}
	for i := 0; i < m.worker.taskDefine.ExecutorCount-1; i++ {
		select {
		case m.notifier <- 1:
		default:
			logrus.Warn("No waiting executor can be notified")
			return
		}
	}
}

func (m *NormalModel) LoopOnce() {
	if m.worker.executeOnceOrReturn() {
		return
	}
	// queue empty
	cur := int(atomic.AddInt32(&m.waiting, 1))
	if cur == m.worker.taskDefine.ExecutorCount {
		// Only last one can fetch new data
		// Release first from waiting avoiding other executors enter again
		atomic.AddInt32(&m.waiting, -1)
		m.worker.selectOnce()
		m.notifyAll()
	} else {
		// block to be notified
		defer atomic.AddInt32(&m.waiting, -1)
		<-m.notifier
	}
}
