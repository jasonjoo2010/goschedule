// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package core

import (
	"time"
)

func (s *ScheduleManager) registerInfo() {
	scheduler, err := s.store.GetScheduler(s.scheduler.Id)
	if err == nil {
		// disabled support
		if s.scheduler.Enabled != scheduler.Enabled {
			s.scheduler.Enabled = scheduler.Enabled
		}
	}
	s.scheduler.LastHeartbeat = s.store.Time()
	s.store.RegisterScheduler(s.scheduler)
}

func (s *ScheduleManager) heartbeat() {
	// stop handler
	defer s.wg.Done()
	defer s.cleanScheduler(s.scheduler.Id)

	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-s.ctx.Done():
			break LOOP
		case <-ticker.C:
			s.registerInfo()
		}
	}
}
