package core

import (
	"log"
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
	heartbeatInterval time.Duration
	// Timeout to be death
	deathTimeout time.Duration
	// Schedule interval
	scheduleInterval time.Duration
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
		heartbeatInterval: 5000 * time.Millisecond,
		deathTimeout:      60000 * time.Millisecond,
		scheduleInterval:  10000 * time.Millisecond,
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
	s.Lock()
	defer s.Unlock()
	if !s.started {
		return
	}
	s.needStop = true
	defer func() {
		s.started = false
	}()

	// wait for heartbeat
	timeout := time.NewTimer(10 * time.Second)
	select {
	case <-s.shutdownNotifier: // heartbeat
	case <-timeout.C:
		log.Println("Failed to stop heartbeat")
	}

	// wait for schedule loop
	timeout.Reset(10 * time.Second)
	select {
	case <-s.shutdownNotifier: // schedule loop
	case <-timeout.C:
		log.Println("Failed to stop schedule loop")
	}
	timeout.Stop()

	s.store.UnregisterScheduler(s.scheduler.Id)
	s.store.Close()
}
