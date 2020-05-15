package worker

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
)

// Worker manages data of scheduling for binded strategy
type Worker interface {
	Start(strategyId, parameter string)
	Stop(strategyId string)
}

var (
	registryMap sync.Map
)

func GetType(name string) reflect.Type {
	if v, ok := registryMap.Load(name); ok {
		t, ok := v.(reflect.Type)
		if ok {
			return t
		}
		logrus.Warn("Type registered for key: ", name, " is not a reflect.Type")
		return nil
	}
	logrus.Warn("No type registered for key: ", name)
	return nil
}

func GetFunc(name string) FuncInterface {
	if v, ok := registryMap.Load(name); ok {
		fmt.Println()
		if fn, ok := v.(FuncInterface); ok {
			return fn
		}
		logrus.Warn("Func registered for key: ", name, " is in incorrect type")
		return nil
	}
	logrus.Warn("No func registered for key: ", name)
	return nil
}

// Register registers specific type with its full package path as key
func Register(worker interface{}) {
	RegisterName(utils.TypeName(worker), worker)
}

// RegisterName registers specific type with specific name as key
func RegisterName(name string, worker interface{}) {
	t := reflect.TypeOf(worker)
	registryMap.Store(name, t)
	logrus.Info("Register new worker type: ", name)
}

// RegisterFunc registers func worker into registry which could be fetch through GetFunc(name string)
func RegisterFunc(name string, fn FuncInterface) {
	registryMap.Store(name, fn)
	logrus.Info("Register new worker func: ", name)
}
