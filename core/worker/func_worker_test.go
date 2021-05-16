// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/stretchr/testify/assert"
)

var counter = 0

func testFunc(strategyId, parameter string) {
	fmt.Println(time.Now().Format(time.RFC3339), "=>", counter)
	counter++
}

func TestFuncWorkerWithInterval(t *testing.T) {
	RegisterFunc("demo", testFunc)
	strategy := definition.Strategy{
		ID:   "s0",
		Kind: definition.FuncKind,
		Bind: "demo",
		Extra: map[string]string{
			"Interval": "1000",
		},
	}
	w, _ := NewFunc(strategy)
	w.Start(strategy.ID, strategy.Parameter)
	time.Sleep(5 * time.Second)
	assert.True(t, counter >= 5)
	w.Stop(strategy.ID, strategy.Parameter)
}

func TestFuncWorkerWithCron(t *testing.T) {
	RegisterFunc("demo", testFunc)
	strategy := definition.Strategy{
		ID:        "s0",
		Kind:      definition.FuncKind,
		Bind:      "demo",
		CronBegin: "*/2 * * * * ?",
		Extra: map[string]string{
			"Interval": "500",
		},
	}
	w, _ := NewFunc(strategy)
	w.Start(strategy.ID, strategy.Parameter)
	time.Sleep(6 * time.Second)
	assert.True(t, counter >= 3)
	w.Stop(strategy.ID, strategy.Parameter)
}
