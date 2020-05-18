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
	Stop(strategyId, parameter string)
}

var (
	registryMap sync.Map
)

func GetInst(name string) Worker {
	if v, ok := registryMap.Load(name); ok {
		t, ok := v.(Worker)
		if ok {
			return t
		}
		logrus.Warn("Instance registered for key: ", name, " is not in correct type")
		return nil
	}
	logrus.Warn("No instance registered for key: ", name)
	return nil
}

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
func Register(worker Worker) {
	RegisterName(utils.TypeName(worker), worker)
}

// RegisterName registers specific type with specific name as key
func RegisterName(name string, worker Worker) {
	t := reflect.TypeOf(worker)
	registryMap.Store(name, t)
	logrus.Info("Register new worker type: ", name)
}

// RegisterInst registers an instance provided instead of its type
func RegisterInst(worker Worker) {
	RegisterInstName(utils.TypeName(worker), worker)
}

// RegisterInstName registers an instance with given name
func RegisterInstName(name string, worker Worker) {
	registryMap.Store(name, worker)
	logrus.Info("Register a worker instance: ", name)
}

// RegisterFunc registers func worker into registry which could be fetch through GetFunc(name string)
func RegisterFunc(name string, fn FuncInterface) {
	registryMap.Store(name, fn)
	logrus.Info("Register new worker func: ", name)
}
