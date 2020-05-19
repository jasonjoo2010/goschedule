package utils

import (
	"time"

	"github.com/robfig/cron/v3"
)

type DelaySupport interface {
	NeedStop() bool
}

// delay guarantees an imprecision delay function
func Delay(target DelaySupport, duration time.Duration) {
	if duration < 1 {
		return
	}
	step := 500 * time.Millisecond
	for duration > 0 {
		if duration < step {
			time.Sleep(duration)
			break
		}
		duration -= step
		time.Sleep(step)
		if target.NeedStop() {
			break
		}
	}
}

// CronDelay add suitable delay according to cron settings
//	when only beginning is set:
//  -----------b-------------b-------------b-------------
//    |------->|
//	when both beginning and ending are set:
//	b=====e------b=====e--------b=====e--------b=====e---
//	   |==|
//	         |-->|=====|        |=====|        |=====|
func CronDelay(target DelaySupport, begin cron.Schedule, end cron.Schedule) {
	if begin != nil && end == nil {
		now := time.Now()
		next := begin.Next(now)
		diff := next.Sub(now)
		Delay(target, diff)
		return
	}
	if begin != nil && end != nil {
		now := time.Now()
		next1 := begin.Next(now)
		next2 := end.Next(now)
		diff1 := next1.Sub(now)
		diff2 := next2.Sub(now)
		if diff2 < diff1 {
			// can running
			return
		}
		Delay(target, diff1)
		return
	}
}
