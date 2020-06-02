// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"time"

	"github.com/jasonjoo2010/goschedule/utils"
)

func (w *TaskWorker) registerTaskRuntime() {
	now := time.Now().Unix() * 1000
	w.runtime.NextRunnable = w.NextBeginTime
	w.runtime.LastHeartBeat = now
	w.runtime.Version++
	w.runtime.Statistics = w.Statistics
	w.store.SetTaskRuntime(&w.runtime)
}

func (w *TaskWorker) heartbeat() {
	// stop handler
	defer func() { w.notifierC <- 2 }()
	for !w.needStop {
		w.registerTaskRuntime()
		utils.Delay(w, time.Duration(w.taskDefine.HeartbeatInterval)*time.Millisecond)
	}
	w.store.RemoveTaskRuntime(w.runtime.StrategyId, w.runtime.TaskId, w.runtime.Id)
}
