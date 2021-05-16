// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package core

import (
	"math"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/jasonjoo2010/goschedule/types"
	"github.com/stretchr/testify/assert"
)

func newManager(t *testing.T, store store.Store) *ScheduleManager {
	cfg := types.ScheduleConfig{
		ScheduleInterval:  100 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		DeathTimeout:      1200 * time.Millisecond,
		StallAfterStartup: 1 * time.Millisecond,
	}
	manager, err := New(cfg, store)
	assert.Nil(t, err)
	assert.NotNil(t, manager)

	return manager
}

func TestHeartbeat(t *testing.T) {
	store := memory.New()
	manager1 := newManager(t, store)
	manager2 := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		ID:      "test",
		IPList:  []string{"127.0.0.1"},
		Enabled: true,
	})

	manager1.Start()
	manager2.Start()

	time.Sleep(time.Second)

	now := time.Now().UnixNano() / int64(time.Millisecond)
	schedulers, err := store.GetSchedulers()
	assert.Nil(t, err)
	for _, s := range schedulers {
		assert.True(t, math.Abs(float64(now-s.LastHeartbeat)) < 200)
		assert.True(t, s.Enabled)
	}

	// runtimes
	runtimeList, err := store.GetStrategyRuntimes("test")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(runtimeList))

	// disable scheduler
	schedulers, err = store.GetSchedulers()
	assert.Nil(t, err)
	for _, s := range schedulers {
		s.Enabled = false
		store.RegisterScheduler(s)
	}

	time.Sleep(time.Second)

	now = time.Now().UnixNano() / int64(time.Millisecond)
	schedulers, err = store.GetSchedulers()
	assert.Nil(t, err)
	for _, s := range schedulers {
		assert.True(t, math.Abs(float64(now-s.LastHeartbeat)) < 200)
		assert.False(t, s.Enabled)
	}

	// runtimes
	runtimeList, err = store.GetStrategyRuntimes("test")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(runtimeList))

	manager1.Shutdown()
	manager2.Shutdown()

	schedulers, err = store.GetSchedulers()
	assert.Empty(t, schedulers)
}
