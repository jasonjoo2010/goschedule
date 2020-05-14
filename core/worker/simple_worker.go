package worker

import (
	"errors"
	"reflect"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/sirupsen/logrus"
)

type SimpleInterface interface {
	Start(strategyId, parameter string)
	Stop(strategyId string)
}

// SimpleWorker triggers Start/Stop to user's implementation
type SimpleWorker struct {
	strategy definition.Strategy
	bind     SimpleInterface
}

func NewSimple(strategy definition.Strategy) (*SimpleWorker, error) {
	t := GetType(strategy.Bind)
	if t == nil {
		logrus.Warn("Create simple worker failed: ", strategy.Bind, " cannot be located")
		return nil, errors.New("No specific type found: " + strategy.Bind)
	}
	w, ok := reflect.New(t).Interface().(SimpleInterface)
	if !ok {
		logrus.Warn("Create simple worker failed: ", strategy.Bind, " cannot be converted to SimpleInterface")
		return nil, errors.New("Convert to SimpleInterface failed: " + strategy.Bind)
	}
	w.Start(strategy.Id, strategy.Parameter)
	logrus.Info("Worker of strategy ", strategy.Id, " started")
	return &SimpleWorker{
		strategy: strategy,
		bind:     w,
	}, nil
}

func (w *SimpleWorker) Stop() {
	w.bind.Stop(w.strategy.Id)
	logrus.Info("Worker of strategy ", w.strategy.Id, " stopped")
}
