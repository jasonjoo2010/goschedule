package task_worker

import (
	"time"

	"github.com/sirupsen/logrus"
)

type SingleExecutor struct {
	worker *TaskWorker
	task   TaskSingle
}

func (m *SingleExecutor) execute(item interface{}) {
	var (
		succ bool
		cost int64
	)
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("Execute error: ", r)
			succ = false
		}
		m.worker.Statistics.Execute(succ, cost)
	}()
	t0 := time.Now()
	succ = m.task.Execute(item, m.worker.ownSign)
	cost = int64(time.Now().Sub(t0) / time.Millisecond)
}

func (m *SingleExecutor) ExecuteOrReturn() bool {
	select {
	case item, ok := <-m.worker.data:
		if ok {
			m.execute(item)
		}
		return true
	default:
		return false
	}
}
