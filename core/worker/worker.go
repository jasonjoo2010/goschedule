package worker

import (
	"reflect"
	"sync"

	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
)

// Worker manages data of scheduling for binded strategy
type Worker interface {
	Stop()
}

var (
	nameToTypeMap sync.Map
)

func GetType(name string) reflect.Type {
	if t, ok := nameToTypeMap.Load(name); ok {
		return t.(reflect.Type)
	}
	logrus.Warn("No type registered for key: ", name)
	return nil
}

// Register register specific type with its full package path as key
func Register(worker interface{}) {
	RegisterName(utils.TypeName(worker), worker)
}

// RegisterName register specific type with specific name as key
func RegisterName(name string, worker interface{}) {
	t := reflect.TypeOf(worker)
	nameToTypeMap.Store(name, t)
	logrus.Info("Register new worker type: ", name)
}
