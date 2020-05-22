package utils

import (
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var (
	cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
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
