package core

import "github.com/jasonjoo2010/goschedule/utils"

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
	defer func() { s.shutdownNotifier <- 1 }()
	for !s.needStop {
		s.registerInfo()
		utils.Delay(s, s.HeartbeatInterval)
	}

	// unregister runtimes from store when stop
	strategies, err := s.store.GetStrategies()
	if err == nil {
		for _, strategy := range strategies {
			s.store.RemoveStrategyRuntime(strategy.Id, s.scheduler.Id)
		}
	}
	// unregister self from store when stop
	s.store.UnregisterScheduler(s.scheduler.Id)
}
