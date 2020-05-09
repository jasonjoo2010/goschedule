package worker

import (
	"log"

	"github.com/jasonjoo2010/goschedule/core/definition"
)

type SimpleWorker struct {
	strategy definition.Strategy
}

func NewSimpe(strategy definition.Strategy) *SimpleWorker {
	// TODO start routines
	log.Println("worker started")
	return &SimpleWorker{
		strategy: strategy,
	}
}

func (w *SimpleWorker) Stop() {
	// TODO
	log.Println("worker stopped")
}
