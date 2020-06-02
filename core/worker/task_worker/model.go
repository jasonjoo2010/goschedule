// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package task_worker

type TaskModel interface {
	LoopOnce()
	Stop() // called when worker stopping
}
