// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package worker

import (
	"testing"

	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/stretchr/testify/assert"
)

type demoSimpleWorker struct {
	started bool
}

func (d *demoSimpleWorker) Start(id, p string) error {
	d.started = true

	return nil
}

func (d *demoSimpleWorker) Stop(id, p string) error {
	d.started = false

	return nil
}

func TestSimpleWorker(t *testing.T) {
	strategy := definition.Strategy{
		Id:   "s0",
		Kind: definition.FuncKind,
		Bind: "demotest",
		Extra: map[string]string{
			"Interval": "1000",
		},
	}
	demo := &demoSimpleWorker{started: true}
	RegisterName("demotest", demo)
	w, _ := NewSimple(strategy)
	assert.IsType(t, &demoSimpleWorker{}, w)
	assert.NotEqual(t, demo, w)
}

func TestSimpleWorkerInst(t *testing.T) {
	strategy := definition.Strategy{
		Id:   "s0",
		Kind: definition.FuncKind,
		Bind: "demotestsingle",
		Extra: map[string]string{
			"Interval": "1000",
		},
	}
	demo := &demoSimpleWorker{}
	RegisterInstName("demotestsingle", demo)
	w, _ := NewSimple(strategy)
	assert.IsType(t, &demoSimpleWorker{}, w)
	assert.Equal(t, demo, w)
}
