// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package definition

import (
	"sync/atomic"
	"time"
)

type Statistics struct {
	LastFetchTime     int64
	SelectCount       int64
	SelectItemCount   int64
	OtherCompareCount int64
	ExecuteSuccCount  int64 // concurrent
	ExecuteFailCount  int64 // concurrent
	ExecuteSpendTime  int64 // concurrent
}

func (s *Statistics) Select(cnt int64) {
	s.LastFetchTime = time.Now().Unix() * 1000
	atomic.AddInt64(&s.SelectCount, 1)
	if cnt > 0 {
		atomic.AddInt64(&s.SelectItemCount, cnt)
	}
}

func (s *Statistics) Execute(succ bool, cost int64) {
	if cost > 0 {
		atomic.AddInt64(&s.ExecuteSpendTime, cost)
	}
	if succ {
		atomic.AddInt64(&s.ExecuteSuccCount, 1)
	} else {
		atomic.AddInt64(&s.ExecuteFailCount, 1)
	}
}
