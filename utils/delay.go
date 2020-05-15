package utils

import "time"

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
