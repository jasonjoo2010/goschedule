// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package worker

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/log"
	"github.com/jasonjoo2010/goschedule/types"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/robfig/cron/v3"
)

// FuncWorker uses a func to implement a task loop. A channel is used to do notifications(ping-pong).
type FuncWorker struct {
	types.Worker

	mu        sync.Mutex
	wg        sync.WaitGroup
	ctx       context.Context
	ctxCancel context.CancelFunc

	strategyId string
	parameter  string
	fn         types.FuncInterface

	schedBegin cron.Schedule
	schedEnd   cron.Schedule
	interval   time.Duration
}

func NewFunc(strategy definition.Strategy) (types.Worker, error) {
	if strategy.Kind != definition.FuncKind {
		return nil, errors.New("Wrong kind of strategy, should be FuncKind")
	}

	fn := GetFunc(strategy.Bind)
	if fn == nil {
		return nil, errors.New("Could not get the binding func")
	}

	w := &FuncWorker{
		fn: fn,
	}

	w.schedBegin, w.schedEnd = utils.ParseStrategyCron(&strategy)
	if strategy.Extra != nil {
		if millisStr, ok := strategy.Extra["Interval"]; ok {
			if millis, err := strconv.Atoi(millisStr); err == nil && millis > 0 {
				w.interval = time.Duration(millis) * time.Millisecond
			}
		}
	}

	log.Infof("Create a func worker, cron=%v, interval=%v", w.schedBegin, w.interval)
	return w, nil
}

func (w *FuncWorker) FuncExecutor(ctx context.Context) {
	defer w.wg.Done()

LOOP:
	for {
		// cron
		delay := utils.CronDelay(w.schedBegin, w.schedEnd)
		if !utils.DelayContext(ctx, delay) {
			break LOOP
		}

		w.fn(w.strategyId, w.parameter)

		if !utils.DelayContext(ctx, w.interval) {
			break LOOP
		}
	}
}

func (w *FuncWorker) Start(strategyId, parameter string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.ctx != nil {
		return errors.New("Worker has already started")
	}

	w.ctx, w.ctxCancel = context.WithCancel(context.Background())
	w.strategyId = strategyId
	w.parameter = parameter

	w.wg.Add(1)
	go w.FuncExecutor(w.ctx)
	return nil
}

func (w *FuncWorker) cleanup() {
	w.ctx = nil
	w.ctxCancel = nil
}

func (w *FuncWorker) Stop(strategyId, parameter string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.ctx == nil {
		return errors.New("Func worker has not been started")
	}
	defer w.cleanup()

	w.ctxCancel()
	w.wg.Wait()
	log.Infof("Worker of strategy %s stopped", strategyId)
	return nil
}
