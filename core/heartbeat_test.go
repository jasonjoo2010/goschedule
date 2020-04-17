package core

import (
	"math"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeat(t *testing.T) {
	store := memory.New()
	manager1, err := New(store)
	manager2, err := New(store)
	assert.Nil(t, err)
	assert.NotNil(t, manager1)
	assert.NotNil(t, manager2)

	// change heartbeat rate manually
	manager1.heartbeatRate = 100 * time.Millisecond
	manager1.deathTimeout = 1000 * time.Millisecond
	manager2.heartbeatRate = 100 * time.Millisecond
	manager2.deathTimeout = 1000 * time.Millisecond

	manager1.Start()
	manager2.Start()

	time.Sleep(time.Second)

	now := time.Now().UnixNano() / int64(time.Millisecond)
	for _, s := range store.GetSchedulers() {
		assert.True(t, math.Abs(float64(now-s.LastHeartbeat)) < 200)
		assert.True(t, s.Enabled)
	}

	// disable scheduler
	for _, s := range store.GetSchedulers() {
		s.Enabled = false
		store.RegisterScheduler(s)
	}

	time.Sleep(time.Second)

	now = time.Now().UnixNano() / int64(time.Millisecond)
	for _, s := range store.GetSchedulers() {
		assert.True(t, math.Abs(float64(now-s.LastHeartbeat)) < 200)
		assert.False(t, s.Enabled)
	}

	manager1.Shutdown()
	manager2.Shutdown()
}
