package worker

import (
	"log"

	"github.com/jasonjoo2010/goschedule/core/definition"
)

type SimpleInterface interface {
	Start(strategyId, parameter string)
	Stop(strategyId string)
}

// SimpleWorker triggers Start/Stop to user's implementation
type SimpleWorker struct {
	strategy definition.Strategy
}

func NewSimple(strategy definition.Strategy) (*SimpleWorker, error) {
	// TODO start routines
	log.Println("worker started")
	return &SimpleWorker{
		strategy: strategy,
	}, nil
}

func (w *SimpleWorker) Stop() {
	// TODO
	log.Println("worker stopped")
}
