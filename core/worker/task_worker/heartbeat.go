package task_worker

import (
	"time"

	"github.com/jasonjoo2010/goschedule/utils"
)

func (w *TaskWorker) registerTaskRuntime() {
	now := time.Now().Unix() * 1000
	w.runtime.LastHeartBeat = now
	w.runtime.Version++
	w.manager.Store().SetTaskRuntime(&w.runtime)
}

func (w *TaskWorker) heartbeat() {
	// stop handler
	defer func() { w.notifier <- 2 }()
	for !w.needStop {
		w.registerTaskRuntime()
		utils.Delay(w, time.Duration(w.taskDefine.HeartbeatInterval)*time.Millisecond)
	}
	w.manager.Store().RemoveTaskRuntime(w.runtime.TaskId, w.runtime.Id)
}
