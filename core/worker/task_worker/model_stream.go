package task_worker

import (
	"sync"

	"golang.org/x/sync/semaphore"
)

// select() -> execute() -> execute() [queue empty] -> select() -> execute()

type StreamModel struct {
	sync.Mutex
	stopped  bool
	sem      *semaphore.Weighted
	worker   *TaskWorker
	notifier chan int
}

func NewStreamModel(worker *TaskWorker) *StreamModel {
	return &StreamModel{
		sem:      semaphore.NewWeighted(1),
		notifier: make(chan int),
		worker:   worker,
	}
}

func (m *StreamModel) notifyAll() {
	m.Lock()
	defer m.Unlock()
	if m.stopped {
		return
	}
	for i := 0; i < m.worker.taskDefine.ExecutorCount-1; i++ {
		select {
		case m.notifier <- 1:
		default:
			return
		}
	}
}

func (m *StreamModel) Stop() {
	m.Lock()
	defer m.Unlock()
	m.stopped = true
	close(m.notifier)
}

func (m *StreamModel) LoopOnce() {
	if m.worker.executeOnceOrReturn() {
		return
	}
	if m.sem.TryAcquire(1) {
		defer m.sem.Release(1)
		m.worker.selectOnce()
		m.notifyAll()
		return
	}
	<-m.notifier
}
