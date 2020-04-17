package core

import "github.com/jasonjoo2010/goschedule/utils"

func (s *ScheduleManager) isLeader() bool {
	list := s.store.GetSchedulers()
	if len(list) < 1 {
		return false
	}
	arr := make([]string, len(list))
	for i, scheduler := range list {
		arr[i] = scheduler.Id
	}
	return utils.IsLeader(arr, s.scheduler.Id)
}

func (s *ScheduleManager) schedule() {
	if !s.isLeader() {
		return
	}
}

func (s *ScheduleManager) scheduleLoop() {
	// stop handler
	defer func() { s.shutdownNotifier <- 2 }()
	for !s.needStop {
		s.schedule()
		s.delay(20000)
	}
}
