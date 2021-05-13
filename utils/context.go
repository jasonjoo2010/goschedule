package utils

import (
	"context"
	"time"
)

// ContextDone returns true if the given context is done
func ContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// LoopContext is used to execute a func with fixed interval.
//	The cleanupFn will be invoked when the loop ends
func LoopContext(ctx context.Context, interval time.Duration, loopFn, cleanupFn func()) {
	defer cleanupFn()

	for !ContextDone(ctx) {
		loopFn()
		if !DelayContext(ctx, interval) {
			break
		}
	}
}
