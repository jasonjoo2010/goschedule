package worker

import (
	"reflect"
	"testing"

	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/stretchr/testify/assert"
)

type Demo struct {
	x, y int
}

func (d *Demo) Start(id, p string) {}
func (d *Demo) Stop(id, p string)  {}

func callback(strategyId, parameter string) {
	// empty
}

func TestRegister(t *testing.T) {
	Register(&Demo{})
	Register(&Demo{})
	assert.NotNil(t, GetType(utils.TypeName(&Demo{})))
	RegisterName("a", &Demo{})
	assert.NotEqual(t, reflect.TypeOf(Demo{}), GetType("a"))
	assert.Equal(t, reflect.TypeOf(&Demo{}), GetType("a"))

	RegisterFunc("a", callback)
	var fn FuncInterface = callback
	assert.IsType(t, fn, GetFunc("a"))
}
