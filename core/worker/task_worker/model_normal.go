package task_worker

import "sync/atomic"

// select() -> execute() -> execute() [queue empty && all done] -> select()

type NormalModel struct {
	worker  *TaskWorker
	waiting int32
}

func NewNormalModel(worker *TaskWorker) *NormalModel {
	return &NormalModel{
		worker: worker,
	}
}

func (m *NormalModel) LoopOnce() {
	if len(m.worker.data) < 1 {
		defer atomic.AddInt32(&m.waiting, -1)
		if atomic.AddInt32(&m.waiting, 1) == int32(m.worker.taskDefine.ExecutorCount) {
			// Only last one can fetch new data
			m.worker.selectOnce()
		} else {
			m.worker.executeOnceOrWait()
		}
		return
	}
	m.worker.executeOnceOrWait()
}
