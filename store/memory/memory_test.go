// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package memory

import (
	"testing"

	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/storetest"
)

func newStorage() store.Store {
	return New()
}

func TestName(t *testing.T) {
	s := newStorage()
	storetest.DoTestName(t, s, "memory")
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

func TestTaskRuntime(t *testing.T) {
	s := newStorage()
	storetest.DoTestTaskRuntime(t, s)
	s.Close()
}

func TestTaskAssignment(t *testing.T) {
	s := newStorage()
	storetest.DoTestTaskAssignment(t, s)
	s.Close()
}

func TestStrategy(t *testing.T) {
	s := newStorage()
	storetest.DoTestStrategy(t, s)
	s.Close()
}

func TestStrategyRuntime(t *testing.T) {
	s := newStorage()
	storetest.DoTestStrategyRuntime(t, s)
	s.Close()
}

func TestScheduler(t *testing.T) {
	s := newStorage()
	storetest.DoTestScheduler(t, s)
	s.Close()
}

func TestTaskReload(t *testing.T) {
	s := newStorage()
	storetest.DoTestTaskReloadItems(t, s)
	s.Close()
}

func TestDump(t *testing.T) {
	s := newStorage()
	storetest.DoTestDump(t, s)
	s.Close()
}
