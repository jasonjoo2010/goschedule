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

func getWorkerFromType(t reflect.Type) Worker {
	if v, ok := reflect.New(t).Interface().(Worker); ok {
		return v
	}
	logrus.Warn("Entry registered is not a convertable type: ", t)
	return nil
}

func GetWorker(name string) Worker {
	var (
		ok bool
		v  interface{}
		t  reflect.Type
		w  Worker
	)
	if v, ok = registryMap.Load(name); !ok {
		logrus.Warn("No type registered for key: ", name)
		return nil
	}
	t, ok = v.(reflect.Type)
	if ok {
		return getWorkerFromType(t)
	}
	w, ok = v.(Worker)
	if ok {
		return w
	}
	logrus.Warn("Type registered for key: ", name, " is not either a type nor inst")
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
	if worker == nil {
		panic("Could not register a worker type using nil as value")
	}
	RegisterName(utils.TypeName(utils.Dereference(worker)), worker)
}

// RegisterName registers specific type with specific name as key
func RegisterName(name string, worker Worker) {
	if name == "" {
		panic("Could not register a worker type without name")
	}
	if worker == nil {
		panic("Could not register a worker type using nil as value")
	}
	t := reflect.TypeOf(utils.Dereference(worker))
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
