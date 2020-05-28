package worker

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// FuncInterface defines the func used in scheduling.
//	Generally it's better keeping invocation fast but if it costs much more time
//	maybe you should carefully set a suitable timeout during shutdown.
type FuncInterface func(strategyId, parameter string)

// FuncWorker uses a func to implement a task loop. A channel is used to do nififications(ping-pong).
type FuncWorker struct {
	sync.Mutex
	strategyId string
	parameter  string
	notifier   chan int
	fn         FuncInterface
	schedBegin cron.Schedule
	schedEnd   cron.Schedule
	interval   time.Duration
	started    bool
	needStop   bool

	// TimeoutShutdown is the timeout when waiting to close the worker
	TimeoutShutdown time.Duration
}

func NewFunc(strategy definition.Strategy) (Worker, error) {
	if strategy.Kind != definition.FuncKind {
		return nil, errors.New("Wrong kind of strategy, should be FuncKind")
	}
	fn := GetFunc(strategy.Bind)
	if fn == nil {
		return nil, errors.New("Could not get the binding func")
	}
	w := &FuncWorker{
		notifier:        make(chan int),
		TimeoutShutdown: 10 * time.Second,
		fn:              fn,
	}
	w.schedBegin, w.schedEnd = utils.ParseStrategyCron(&strategy)
	if strategy.Extra != nil {
		if millisStr, ok := strategy.Extra["Interval"]; ok {
			if millis, err := strconv.Atoi(millisStr); err == nil && millis > 0 {
				w.interval = time.Duration(millis) * time.Millisecond
			}
		}
	}
	logrus.Info("Create a func worker, cron=", w.schedBegin, ", interval=", w.interval/time.Millisecond)
	return w, nil
}

func (w *FuncWorker) NeedStop() bool {
	return w.needStop
}

func (w *FuncWorker) FuncExecutor() {
	for {
		// cron
		utils.CronDelay(w, w.schedBegin, w.schedEnd)
		w.fn(w.strategyId, w.parameter)
		if w.interval > 0 {
			utils.Delay(w, w.interval)
		}
		if w.needStop {
			break
		}
	}
	w.notifier <- 1
	w.started = false
	w.needStop = false
}

func (w *FuncWorker) Start(strategyId, parameter string) {
	w.Lock()
	defer w.Unlock()
	if w.started {
		logrus.Warn("Worker has already started, ignore")
		return
	}
	w.started = true
	w.strategyId = strategyId
	w.parameter = parameter
	go w.FuncExecutor()
}

func (w *FuncWorker) Stop(strategyId, parameter string) {
	w.needStop = true
	timeout := time.NewTimer(w.TimeoutShutdown)
	select {
	case <-w.notifier:
		// succ
		timeout.Stop()
	case <-timeout.C:
		// timeout
		logrus.Error("Failed to stop a FuncWorker")
	}
	logrus.Info("Worker of strategy ", strategyId, " stopped")
}
