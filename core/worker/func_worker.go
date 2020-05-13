package worker

import (
	"log"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
)

type FuncWorkerFunction func(strategyId, parameter string, closeNotifier chan int)

// FuncWorker uses a func to implement a task loop. A channel is used to do nififications(ping-pong).
type FuncWorker struct {
	strategy definition.Strategy
	notifier chan int
}

func NewFunc(strategy definition.Strategy) (*FuncWorker, error) {
	// TODO start routines
	log.Println("worker started")
	return &FuncWorker{
		strategy: strategy,
		notifier: make(chan int),
	}, nil
}

func (w *FuncWorker) Stop() {
	timeout := time.Timer{}
	time.After(10 * time.Second)
	select {
	case w.notifier <- 1:
		// succ
		timeout.Stop()
	case <-timeout.C:
		// timeout
		log.Fatalln("Failed to stop a FuncWorker")
	}
	log.Println("worker stopped")
}
