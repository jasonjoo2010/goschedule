package core

import (
	"sync"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
)

type ScheduleManager struct {
	sync.Mutex
	store            store.Store
	scheduler        *definition.Scheduler
	workersMap       map[string][]worker.Worker
	shutdownNotifier chan int
	needStop         bool
	Started          bool
	// Interval of heartbeat
	HeartbeatInterval time.Duration
	// Timeout to be death
	DeathTimeout time.Duration
	// Schedule interval
	ScheduleInterval time.Duration
	// Timeout when trying to shutdown
	ShutdownTimeout time.Duration
}

func New(store store.Store) (*ScheduleManager, error) {
	// generate uuid
	seq, err := store.Sequence()
	if err != nil {
		return nil, err
	}
	uuid := utils.GenerateUUID(seq)
	s := &definition.Scheduler{
		Id:      uuid,
		Enabled: true,
	}
	m := &ScheduleManager{
		store:             store,
		shutdownNotifier:  make(chan int),
		scheduler:         s,
		workersMap:        make(map[string][]worker.Worker),
		HeartbeatInterval: 5000 * time.Millisecond,
		DeathTimeout:      60000 * time.Millisecond,
		ScheduleInterval:  10000 * time.Millisecond,
		ShutdownTimeout:   10000 * time.Millisecond,
	}
	return m, nil
}

func (s *ScheduleManager) NeedStop() bool {
	return s.needStop
}

func (s *ScheduleManager) Store() store.Store {
	return s.store
}

func (s *ScheduleManager) Scheduler() definition.Scheduler {
	return *s.scheduler
}

func (s *ScheduleManager) Start() {
	s.Lock()
	defer s.Unlock()
	if s.Started {
		return
	}
	s.Started = true
	go s.heartbeat()
	go s.scheduleLoop()
}

func (s *ScheduleManager) Shutdown() {
	s.Lock()
	defer s.Unlock()
	if !s.Started {
		return
	}
	s.needStop = true
	defer func() {
		s.Started = false
	}()

	// wait for heartbeat
	timeout := time.NewTimer(s.ShutdownTimeout)
	select {
	case <-s.shutdownNotifier: // heartbeat
	case <-timeout.C:
		logrus.Warn("Failed to stop heartbeat")
	}

	// wait for schedule loop
	timeout.Reset(10 * time.Second)
	select {
	case <-s.shutdownNotifier: // schedule loop
	case <-timeout.C:
		logrus.Warn("Failed to stop schedule loop")
	}
	timeout.Stop()

	s.cleanScheduler(s.scheduler.Id)
	s.store.Close()
}
