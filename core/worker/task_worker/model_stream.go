package task_worker

import (
	"golang.org/x/sync/semaphore"
)

// select() -> execute() -> execute() [queue empty] -> select() -> execute()

type StreamModel struct {
	sem    *semaphore.Weighted
	worker *TaskWorker
}

func NewStreamModel(worker *TaskWorker) *StreamModel {
	return &StreamModel{
		sem:    semaphore.NewWeighted(1),
		worker: worker,
	}
}

func (m *StreamModel) LoopOnce() {
	if len(m.worker.data) < 1 {
		// fill it
		if m.sem.TryAcquire(1) {
			defer m.sem.Release(1)
			m.worker.requestSelecting()
			return
		}
	}
	m.worker.executeOnceOrWait()
}
