// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"context"
	"time"

	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var (
	cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
)

// DelayContext try to wait and return true if normally completed.
func DelayContext(ctx context.Context, duration time.Duration) bool {
	if duration < 1 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	timeout := time.NewTimer(duration)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timeout.C:
		return true
	}
}

func parseCron(cronStr string) cron.Schedule {
	if cronStr == "" {
		return nil
	}
	sched, err := cronParser.Parse(cronStr)
	if err != nil {
		logrus.Warn("Cron expression parsing failed: ", err.Error())
		return nil
	}
	return sched
}

// ParseStrategyCron parse cron expressions of begin and end from given strategy
func ParseStrategyCron(strategy *definition.Strategy) (cron.Schedule, cron.Schedule) {
	return parseCron(strategy.CronBegin), parseCron(strategy.CronEnd)
}

// CronDelay add suitable delay according to cron settings
//	when only beginning is set:
//  -----------b-------------b-------------b-------------
//    |------->|
//	when both beginning and ending are set:
//	b=====e------b=====e--------b=====e--------b=====e---
//	   |==|
//	         |-->|=====|        |=====|        |=====|
func CronDelay(begin cron.Schedule, end cron.Schedule) time.Duration {
	if begin != nil && end == nil {
		now := time.Now()
		next := begin.Next(now)
		diff := next.Sub(now)
		return diff
	}
	if begin != nil && end != nil {
		now := time.Now()
		next1 := begin.Next(now)
		next2 := end.Next(now)
		diff1 := next1.Sub(now)
		diff2 := next2.Sub(now)
		if diff2 < diff1 {
			// can running
			return 0
		}
		return diff1
	}
	return 0
}
