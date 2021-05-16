// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package core

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	u "github.com/jasonjoo2010/goschedule/core/utils"
	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/log"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/types"
	"github.com/jasonjoo2010/goschedule/utils"
)

type ScheduleManager struct {
	io.Closer

	mu        sync.Mutex
	wg        sync.WaitGroup
	cfg       types.ScheduleConfig
	ctx       context.Context
	ctxCancel context.CancelFunc

	store     store.Store
	scheduler *definition.Scheduler

	workerSet *u.WorkerSet
}

func initCfg(cfg *types.ScheduleConfig) error {
	if cfg.DeathTimeout <= 0 {
		cfg.DeathTimeout = 60 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.StallAfterStartup <= 0 {
		cfg.StallAfterStartup = 10 * time.Second
	}
	if cfg.ScheduleInterval <= 0 {
		cfg.ScheduleInterval = 10 * time.Second
	}

	if cfg.HeartbeatInterval*2 > cfg.DeathTimeout {
		return errors.New("Heartbeat interval should be no more than half of the death timeout")
	}

	return nil
}

func New(cfg types.ScheduleConfig, store store.Store) (*ScheduleManager, error) {
	if err := initCfg(&cfg); err != nil {
		return nil, err
	}

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
		store:     store,
		scheduler: s,
		workerSet: u.NewWorkerSet(),
		cfg:       cfg,
	}
	return m, nil
}

func (s *ScheduleManager) Store() store.Store {
	return s.store
}

func (s *ScheduleManager) Scheduler() definition.Scheduler {
	return *s.scheduler
}

func (s *ScheduleManager) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx != nil {
		return errors.New("Manager has already been started")
	}

	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	s.wg.Add(2)
	go utils.LoopContext(s.ctx,
		s.cfg.HeartbeatInterval,
		s.registerInfo,
		func() {
			defer s.wg.Done()
			defer s.cleanScheduler(s.scheduler.Id)
		})
	go utils.LoopContext(s.ctx,
		s.cfg.ScheduleInterval,
		s.schedule,
		func() {
			defer s.wg.Done()
			defer s.stopAllWorkers()
		})
	return nil
}

func (s *ScheduleManager) Shutdown() {
	s.Close()
}

func (s *ScheduleManager) cleanup() {
	s.cleanScheduler(s.scheduler.Id)
	if err := s.store.Close(); err != nil {
		log.Errorf("Close store failed: %s", err.Error())
	}

	log.Info("Manager has been shutdown")
}

// Shutdown close the manager. Please use `Close()` instead
func (s *ScheduleManager) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if utils.ContextDone(s.ctx) {
		return errors.New("Manager has already been closed")
	}
	s.ctxCancel()
	defer s.cleanup()

	if s.cfg.ShutdownTimeout == 0 {
		s.wg.Wait()
		return nil
	}

	// wait with a timeout
	notifyC := make(chan int, 1)
	timeout := time.NewTimer(s.cfg.ShutdownTimeout)

	go func() {
		// wait for heartbeat and schedule loops to stop
		s.wg.Wait()
		close(notifyC)
	}()

	select {
	case <-notifyC:
		timeout.Stop()
		return nil
	case <-timeout.C:
		log.Warn("Fail to wait for all loops to stop, force to quit")
		return nil
	}
}
