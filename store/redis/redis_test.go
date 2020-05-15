package redis

import (
	"testing"

	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/storetest"
)

func newStorage() store.Store {
	return NewFromConfig(&RedisStoreConfig{
		Addrs:  []string{"127.0.0.1:6379"},
		Prefix: "schedule-demo",
	})
}

func TestName(t *testing.T) {
	s := newStorage()
	storetest.DoTestName(t, s, "redis")
	s.Close()
}

func TestTime(t *testing.T) {
	s := newStorage()
	storetest.DoTestTime(t, s)
	s.Close()
}

func TestSequence(t *testing.T) {
	s := newStorage()
	storetest.DoTestSequence(t, s)
	s.Close()
}

func TestTask(t *testing.T) {
	s := newStorage()
	storetest.DoTestTask(t, s)
	s.Close()
}

func TestStrategy(t *testing.T) {
	s := newStorage()
	storetest.DoTestStrategy(t, s)
	s.Close()
}

func TestScheduler(t *testing.T) {
	s := newStorage()
	storetest.DoTestScheduler(t, s)
	s.Close()
}

func TestDump(t *testing.T) {
	s := newStorage()
	storetest.DoTestDump(t, s)
	s.Close()
}
