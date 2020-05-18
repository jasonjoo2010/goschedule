package worker

import (
	"errors"
	"reflect"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/sirupsen/logrus"
)

func NewSimple(strategy definition.Strategy) (Worker, error) {
	if strategy.SingleInstance {
		w := GetInst(strategy.Bind)
		if w == nil {
			logrus.Warn("Fetch simple worker instance failed for ", strategy.Bind)
			return nil, errors.New("No specific instance found: " + strategy.Bind)
		}
		return w, nil
	} else {
		t := GetType(strategy.Bind)
		if t == nil {
			logrus.Warn("Create simple worker failed: ", strategy.Bind, " cannot be located")
			return nil, errors.New("No specific type found: " + strategy.Bind)
		}
		w, ok := reflect.New(t).Elem().Interface().(Worker)
		if !ok {
			logrus.Warn("Create simple worker failed: ", strategy.Bind, " cannot be converted to Worker")
			return nil, errors.New("Convert to Worker failed: " + strategy.Bind)
		}
		logrus.Info("Worker of strategy ", strategy.Id, " created")
		return w, nil
	}
}
