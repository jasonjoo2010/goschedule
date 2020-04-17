package core

func (s *ScheduleManager) reregisterInfo() {
	scheduler, err := s.store.GetScheduler(s.scheduler.Id)
	if err == nil {
		// disabled support
		if s.scheduler.Enabled != scheduler.Enabled {
			s.scheduler.Enabled = scheduler.Enabled
		}
	}
	s.scheduler.LastHeartbeat = s.store.Time()
	s.store.RegisterScheduler(s.scheduler)
	if s.scheduler.Enabled == false {
		// TODO stop all servers locally
	}
}

func (s *ScheduleManager) heartbeat() {
	// stop handler
	defer func() { s.shutdownNotifier <- 1 }()
	for !s.needStop {
		go s.reregisterInfo()
		s.delay(s.heartbeatRate)
	}
}
