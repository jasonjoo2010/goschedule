package worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
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
		Id:   "s0",
		Kind: definition.FuncKind,
		Bind: "demo",
		Extra: map[string]string{
			"Interval": "1000",
		},
	}
	w, _ := NewFunc(strategy)
	w.Start(strategy.Id, strategy.Parameter)
	time.Sleep(5 * time.Second)
	assert.True(t, counter >= 5)
	w.Stop(strategy.Id)
}

func TestFuncWorkerWithCron(t *testing.T) {
	RegisterFunc("demo", testFunc)
	strategy := definition.Strategy{
		Id:   "s0",
		Kind: definition.FuncKind,
		Bind: "demo",
		Extra: map[string]string{
			"Interval": "500",
			"Cron":     "*/2 * * * * ?",
		},
	}
	w, _ := NewFunc(strategy)
	w.Start(strategy.Id, strategy.Parameter)
	time.Sleep(6 * time.Second)
	assert.True(t, counter >= 3)
	w.Stop(strategy.Id)
}
