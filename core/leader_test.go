// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package core

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/stretchr/testify/assert"
)

type DemoWorker struct {
	cnt int32
}

func (demo *DemoWorker) Start(strategyId, parameter string) error {
	time.Sleep(500 * time.Millisecond)
	fmt.Println("start for", strategyId)
	atomic.AddInt32(&demo.cnt, 1)

	return nil
}

func (demo *DemoWorker) Stop(strategyId, parameter string) error {
	time.Sleep(500 * time.Millisecond)
	fmt.Println("stop for", strategyId)
	atomic.AddInt32(&demo.cnt, -1)

	return nil
}

func TestStopAllWorkers(t *testing.T) {
	// stop parellel
	store := memory.New()
	manager := newManager(t, store)
	demo := &DemoWorker{}
	worker.RegisterInstName("demoStopWorker", demo)
	store.CreateStrategy(&definition.Strategy{
		Id:      "s0",
		IpList:  []string{"localhost"},
		Enabled: true,
		Kind:    definition.SimpleKind,
		Bind:    "demoStopWorker",
		Total:   50,
	})
	store.CreateStrategy(&definition.Strategy{
		Id:      "s1",
		IpList:  []string{"localhost"},
		Enabled: true,
		Kind:    definition.SimpleKind,
		Bind:    "demoStopWorker",
		Total:   50,
	})
	assert.Nil(t, manager.Start())
	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(100), demo.cnt)

	assert.Nil(t, manager.Close())
	assert.Equal(t, int32(0), demo.cnt)
}

func TestLeader(t *testing.T) {
	store := memory.New()
	manager1 := newManager(t, store)
	manager2 := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		Id:      "s0",
		IpList:  []string{"localhost"},
		Enabled: true,
	})

	assert.False(t, manager1.isLeader("s0"))
	assert.False(t, manager2.isLeader("s0"))

	manager1.registerInfo()
	manager1.generateRuntimes()

	time.Sleep(time.Second)
	assert.True(t, manager1.isLeader("s0"))
	assert.False(t, manager2.isLeader("s0"))

	manager1.registerInfo()
	manager2.generateRuntimes()

	isLeader1 := manager1.isLeader("s0")
	isLeader2 := manager2.isLeader("s0")
	assert.False(t, isLeader1 && isLeader2)
	assert.True(t, isLeader1 || isLeader2)
}

func TestExpiredSchedulers(t *testing.T) {
	store := memory.New()
	manager := newManager(t, store)
	managerExpired := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		Id:      "s0",
		IpList:  []string{"localhost"},
		Enabled: true,
	})

	manager.Start()

	time.Sleep(200 * time.Millisecond)
	list, _ := store.GetSchedulers()
	assert.Equal(t, 1, len(list))

	// Register an expired one
	managerExpired.registerInfo()
	managerExpired.generateRuntimes()
	list, _ = store.GetSchedulers()
	assert.Equal(t, 2, len(list))

	runtimes, _ := store.GetStrategyRuntimes("s0")
	assert.Equal(t, 2, len(runtimes))

	// Wait to cleared
	time.Sleep(manager.cfg.DeathTimeout + manager.cfg.ScheduleInterval)

	list, _ = store.GetSchedulers()
	assert.Equal(t, 1, len(list))

	runtimes, _ = store.GetStrategyRuntimes("s0")
	assert.Equal(t, 1, len(runtimes))

	manager.Shutdown()
}

func TestGenerateRuntimes(t *testing.T) {
	store := memory.New()
	manager := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		Id:      "s0",
		IpList:  []string{"localhost"},
		Enabled: true,
	})
	store.CreateStrategy(&definition.Strategy{
		Id:      "s1",
		IpList:  []string{"127.0.0.1"},
		Enabled: true,
	})

	list, _ := store.GetStrategyRuntimes("s0")
	assert.Equal(t, 0, len(list))
	list, _ = store.GetStrategyRuntimes("s1")
	assert.Equal(t, 0, len(list))

	manager.generateRuntimes()
	list, _ = store.GetStrategyRuntimes("s0")
	assert.Equal(t, 1, len(list))
	list, _ = store.GetStrategyRuntimes("s1")
	assert.Equal(t, 1, len(list))
}

func TestAssign(t *testing.T) {
	worker.RegisterName("demo", &DemoWorker{})
	store := memory.New()
	manager1 := newManager(t, store)
	manager2 := newManager(t, store)
	manager3 := newManager(t, store)
	manager1.cfg.ScheduleInterval = 200 * time.Millisecond
	manager2.cfg.ScheduleInterval = 200 * time.Millisecond
	manager3.cfg.ScheduleInterval = 200 * time.Millisecond
	manager1.cfg.StallAfterStartup = 0
	manager2.cfg.StallAfterStartup = 0
	manager3.cfg.StallAfterStartup = 0

	TASK_COUNT := 2

	store.CreateStrategy(&definition.Strategy{
		Id:                   "S",
		IpList:               []string{"127.0.0.1"},
		Total:                TASK_COUNT,
		Kind:                 definition.SimpleKind,
		Bind:                 "demo",
		MaxOnSingleScheduler: 1,
		Enabled:              true,
	})

	manager1.Start()
	manager2.Start()
	manager3.Start()

	time.Sleep(time.Second)
	runtimes, _ := store.GetStrategyRuntimes("S")
	total := 0
	for _, r := range runtimes {
		total += r.Num
	}
	assert.Equal(t, TASK_COUNT, total)

	manager1.Shutdown()
	manager2.Shutdown()
	manager3.Shutdown()
}
