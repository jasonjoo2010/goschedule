package core

import "time"

// delay guarantees an imprecision delay function
func (s *ScheduleManager) delay(duration time.Duration) {
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
		if s.needStop {
			break
		}
	}
}
