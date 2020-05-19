package utils

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
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

func TestCronDelay(t *testing.T) {
	demo := &DelayDemo{}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	begin, _ := parser.Parse("0/3 * * * * ?")
	end, _ := parser.Parse("1/3 * * * * ?")
	assert.NotNil(t, begin)
	assert.NotNil(t, end)

	now := time.Now()
	diff := begin.Next(now).Sub(now) + 2*time.Millisecond
	if diff > 0 {
		time.Sleep(diff)
	}
	t0 := time.Now()
	CronDelay(demo, begin, nil)
	t1 := time.Now()
	assert.True(t, t1.Sub(t0) > 2500*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 3100*time.Millisecond)

	now = time.Now()
	diff = begin.Next(now).Sub(now) + 2*time.Millisecond
	if diff > 0 {
		time.Sleep(diff)
	}
	t0 = time.Now()
	CronDelay(demo, begin, end)
	t1 = time.Now()
	assert.True(t, t1.Sub(t0) < 100*time.Millisecond)

	now = time.Now()
	diff = end.Next(now).Sub(now) + 2*time.Millisecond
	if diff > 0 {
		time.Sleep(diff)
	}
	t0 = time.Now()
	CronDelay(demo, begin, end)
	t1 = time.Now()
	assert.True(t, t1.Sub(t0) > 1900*time.Millisecond)
}
