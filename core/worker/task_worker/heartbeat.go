// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

import (
	"time"
)

func (w *TaskWorker) registerTaskRuntime() {
	now := time.Now().Unix() * 1000
	w.runtime.NextRunnable = w.NextBeginTime
	w.runtime.LastHeartbeat = now
	w.runtime.Version++
	w.runtime.Statistics = w.Statistics
	w.store.SetTaskRuntime(&w.runtime)
}
