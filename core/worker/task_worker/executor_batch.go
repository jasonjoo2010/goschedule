package task_worker

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type BatchExecutor struct {
	worker *TaskWorker
	task   TaskBatch
	pool   sync.Pool
}

func (m *BatchExecutor) execute(items []interface{}) {
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
	succ = m.task.Execute(items, m.worker.ownSign)
	cost = int64(time.Now().Sub(t0) / time.Millisecond)
}

func (m *BatchExecutor) ExecuteOrReturn() {
	var (
		ok   bool
		item interface{}
	)
	select {
	case item, ok = <-m.worker.data:
	default:
		return
	}
	if ok {
		// try to fill arr
		items := m.pool.Get().([]interface{})
		items = append(items, item)
	LOOP:
		for len(items) < m.worker.taskDefine.BatchCount {
			select {
			case item, ok = <-m.worker.data:
				if ok {
					items = append(items, item)
				} else {
					break LOOP
				}
			default:
				break LOOP
			}
		}
		if len(items) > 0 {
			m.execute(items)
			m.pool.Put(items[:0])
		}
	}
}

func (m *BatchExecutor) ExecuteOrWait() {
	item, ok := <-m.worker.data
	if ok {
		// try to fill arr
		items := m.pool.Get().([]interface{})
		items = append(items, item)
	LOOP:
		for len(items) < m.worker.taskDefine.BatchCount {
			select {
			case item, ok = <-m.worker.data:
				if ok {
					items = append(items, item)
				} else {
					break LOOP
				}
			default:
				break LOOP
			}
		}
		m.execute(items)
		m.pool.Put(items[:0])
	}
}
