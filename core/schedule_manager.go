package core

import (
	"sync"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/utils"
)

type ScheduleManager struct {
	sync.Mutex
	store            store.Store
	scheduler        *definition.Scheduler
	shutdownNotifier chan int
	started          bool
	needStop         bool
	// Interval of heartbeat
	heartbeatRate time.Duration
	// Timeout to be death
	deathTimeout time.Duration
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
		store:            store,
		shutdownNotifier: make(chan int),
		scheduler:        s,
		heartbeatRate:    5000 * time.Millisecond,
		deathTimeout:     60000 * time.Millisecond,
	}
	return m, nil
}

func (s *ScheduleManager) Start() {
	s.Lock()
	defer s.Unlock()
	if s.started {
		return
	}
	s.started = true
	go s.heartbeat()
	go s.scheduleLoop()
}

func (s *ScheduleManager) Shutdown() {
	s.needStop = true
	<-s.shutdownNotifier // heartbeat
	<-s.shutdownNotifier // schedule loop
	defer func() {
		s.started = false
	}()
	s.store.UnregisterScheduler(s.scheduler.Id)
	s.store.Close()
}
