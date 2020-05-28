package worker

import (
	"testing"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/stretchr/testify/assert"
)

type demoSimpleWorker struct {
	started bool
}

func (d *demoSimpleWorker) Start(id, p string) {
	d.started = true
}

func (d *demoSimpleWorker) Stop(id, p string) {
	d.started = false
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
	demo := &demoSimpleWorker{}
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
