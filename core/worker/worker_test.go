// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package worker

import (
	"reflect"
	"testing"

	"github.com/jasonjoo2010/goschedule/types"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/stretchr/testify/assert"
)

type Demo struct {
	x, y int
}

func (d *Demo) Start(id, p string) error { return nil }
func (d *Demo) Stop(id, p string) error  { return nil }

func callback(strategyId, parameter string) {
	// empty
}

func TestRegister(t *testing.T) {
	RegisterName("bbb", &Demo{})
	Register(&Demo{})
	Register(&Demo{})
	assert.NotNil(t, GetWorker(utils.TypeName(Demo{})))
	RegisterName("a", &Demo{1, 2})
	assert.Equal(t, reflect.TypeOf(&Demo{}), reflect.TypeOf(GetWorker("a")))
	assert.NotEqual(t, &Demo{1, 2}, GetWorker("a"))

	RegisterFunc("a", callback)
	var fn types.FuncInterface = callback
	assert.IsType(t, fn, GetFunc("a"))

	demo := &Demo{1, 2}
	RegisterInst(demo)
	RegisterInstName("demo", demo)
	assert.NotNil(t, GetWorker(utils.TypeName(demo)))
	assert.NotNil(t, GetWorker("demo"))

	newDemo, ok := GetWorker("demo").(*Demo)
	assert.True(t, ok)
	assert.Equal(t, 1, newDemo.x)
	assert.Equal(t, 2, newDemo.y)
}
