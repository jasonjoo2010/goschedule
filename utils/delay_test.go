// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"context"
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

func TestDelayContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	t0 := time.Now().UnixNano()
	succ := DelayContext(ctx, 600*time.Millisecond)
	cancel()
	t1 := time.Now().UnixNano()
	diff := (t1 - t0) / 1e6
	assert.True(t, diff > 90)
	assert.True(t, diff < 110)
	assert.False(t, succ)

	t0 = time.Now().UnixNano()
	succ = DelayContext(ctx, 600*time.Millisecond)
	t1 = time.Now().UnixNano()
	diff = (t1 - t0) / 1e6
	assert.True(t, diff < 10)
	assert.False(t, succ)

	{
		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		t0 = time.Now().UnixNano()
		succ = DelayContext(ctx, 60*time.Millisecond)
		t1 = time.Now().UnixNano()
		diff = (t1 - t0) / 1e6
		assert.True(t, diff > 50)
		assert.True(t, diff < 70)
		assert.True(t, succ)

		t0 = time.Now().UnixNano()
		succ = DelayContext(ctx, 60*time.Millisecond)
		cancel()
		t1 = time.Now().UnixNano()
		diff = (t1 - t0) / 1e6
		assert.True(t, diff > 30)
		assert.True(t, diff < 50)
		assert.False(t, succ)
	}
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
	delay := CronDelay(begin, nil)
	assert.True(t, delay > 2500*time.Millisecond)
	assert.True(t, delay < 3100*time.Millisecond)

	now = time.Now()
	diff = begin.Next(now).Sub(now) + 2*time.Millisecond
	if diff > 0 {
		time.Sleep(diff)
	}
	delay = CronDelay(begin, end)
	assert.True(t, delay == 0)

	now = time.Now()
	diff = end.Next(now).Sub(now) + 2*time.Millisecond
	if diff > 0 {
		time.Sleep(diff)
	}
	delay = CronDelay(begin, end)
	assert.True(t, delay > 1900*time.Millisecond)
}
