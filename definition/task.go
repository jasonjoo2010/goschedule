// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package definition

import "encoding/json"

// Sleep Model:
//	t0        t1        t2        t3
//	fetch()   *         *         *
//  exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	done      done      exe()     done
//  done      done      fetch()   done
//	exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	exe()     done      done      done
//	fetch()   done      done      done
//	.......

// NoSleep Model:
//	t0        t1        t2        t3
//	fetch()   *         *         *
//  exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	done      exe()     exe()     exe()
//	fetch()   exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	.......

type Model int

const (
	Normal Model = iota
	Stream
)

type Task struct {
	ID             string
	IntervalNoData int // Whether to delay specified time (millis) if no data selected
	Interval       int // Whether to delay specified time (millis) if something are selected out
	FetchCount     int
	BatchCount     int // If implement TaskBatch the maximum tasks in one call
	ExecutorCount  int // 1 selector -> N executor(s)
	Model          Model
	Parameter      string // Parameter of task
	Bind           string // Bond to registry
	Items          []TaskItem
	MaxTaskItems   int // max task items per Worker

	// Interval of heartbeat, in millis
	HeartbeatInterval int
	// Timeout to be death, in millis
	DeathTimeout int
}

func (t *Task) String() string {
	data, _ := json.Marshal(t)
	return string(data)
}
