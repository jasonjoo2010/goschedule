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
	if m.worker.executeOnceOrReturn() {
		return
	}
	// queue empty
	cur := int(atomic.AddInt32(&m.waiting, 1))
	defer atomic.AddInt32(&m.waiting, -1)
	if cur == m.worker.taskDefine.ExecutorCount {
		// Only last one can fetch new data
		m.worker.selectOnce()
	} else {
		m.worker.executeOnceOrWait()
	}
	return
}
