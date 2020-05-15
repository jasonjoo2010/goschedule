package worker

import (
	"errors"
	"reflect"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/sirupsen/logrus"
)

func NewSimple(strategy definition.Strategy) (Worker, error) {
	t := GetType(strategy.Bind)
	if t == nil {
		logrus.Warn("Create simple worker failed: ", strategy.Bind, " cannot be located")
		return nil, errors.New("No specific type found: " + strategy.Bind)
	}
	w, ok := reflect.New(t).Interface().(Worker)
	if !ok {
		logrus.Warn("Create simple worker failed: ", strategy.Bind, " cannot be converted to SimpleInterface")
		return nil, errors.New("Convert to SimpleInterface failed: " + strategy.Bind)
	}
	logrus.Info("Worker of strategy ", strategy.Id, " created")
	return w, nil
}
