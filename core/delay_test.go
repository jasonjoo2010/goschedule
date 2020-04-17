package core

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/stretchr/testify/assert"
)

func TestDelay(t *testing.T) {
	store := memory.New()
	manager, _ := New(store)
	manager.Start()

	t0 := time.Now().UnixNano()
	manager.delay(60 * time.Millisecond)
	t1 := time.Now().UnixNano()
	diff := (t1 - t0) / 1e6
	assert.True(t, diff > 50)
	assert.True(t, diff < 70)

	t0 = time.Now().UnixNano()
	manager.delay(2 * time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 10)

	t0 = time.Now().UnixNano()
	manager.delay(1010 * time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 1020*1.1)
	assert.True(t, diff > 1000*0.9)

	t0 = time.Now().UnixNano()
	manager.delay(2010 * time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 2010*1.1)
	assert.True(t, diff > 2010*0.9)

	manager.Shutdown()
}
