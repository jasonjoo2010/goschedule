package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type DelayDemo struct {
	needStop bool
}

func (demo *DelayDemo) NeedStop() bool {
	return demo.needStop
}

func TestDelay(t *testing.T) {
	demo := &DelayDemo{}

	t0 := time.Now().UnixNano()
	Delay(demo, 60*time.Millisecond)
	t1 := time.Now().UnixNano()
	diff := (t1 - t0) / 1e6
	assert.True(t, diff > 50)
	assert.True(t, diff < 70)

	t0 = time.Now().UnixNano()
	Delay(demo, 2*time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 10)

	t0 = time.Now().UnixNano()
	Delay(demo, 1010*time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 1020*1.1)
	assert.True(t, diff > 1000*0.9)

	t0 = time.Now().UnixNano()
	Delay(demo, 2010*time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 2010*1.1)
	assert.True(t, diff > 2010*0.9)
}
