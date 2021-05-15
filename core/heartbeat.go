// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package core

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
