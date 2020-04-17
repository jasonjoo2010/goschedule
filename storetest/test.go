package storetest

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/stretchr/testify/assert"
)

func DoTestName(t *testing.T, s store.Store, name string) {
	assert.Equal(t, name, s.Name())
}

func DoTestTime(t *testing.T, s store.Store) {
	t0 := s.Time()
	time.Sleep(time.Millisecond * 200)
	t1 := s.Time()
	assert.True(t, t0 > 0)
	assert.True(t, t1 > 0)
	assert.True(t, t1 > t0)
	assert.True(t, (t1-t0) > 190 && (t1-t0) < 210)
}

func DoTestSequence(t *testing.T, s store.Store) {
	s0, _ := s.Sequence()
	s1, _ := s.Sequence()
	assert.True(t, s0 > 0)
	assert.True(t, s1 > 0)
	assert.True(t, s1 > s0)
	assert.True(t, (s1-s0) == 1)
}

func DoTestTask(t *testing.T, s store.Store) {
	taskOri := &definition.Task{
		Id:      "demo-task",
		Threads: 3,
	}

	// try to fetch not existed task
	task, err := s.GetTask(taskOri.Id)
	assert.Nil(t, task)
	assert.Equal(t, store.NotExist, err)

	// try to update not existed task
	err = s.UpdateTask(taskOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)

	// create
	err = s.CreateTask(taskOri)
	assert.Nil(t, err)

	// verify creation
	task, err = s.GetTask(taskOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, taskOri.Id, task.Id)

	// recreation
	err = s.CreateTask(taskOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.AlreadyExist, err)

	// task list
	arr, err := s.GetTasks()
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, taskOri.Id, arr[0].Id)

	// modify
	taskOri.Threads = 44
	err = s.UpdateTask(taskOri)
	assert.Nil(t, err)

	// verify modify
	task, err = s.GetTask(taskOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, taskOri.Id, task.Id)
	assert.Equal(t, 44, task.Threads)

	// delete
	err = s.DeleteTask(taskOri.Id)
	assert.Nil(t, err)

	// re-delete
	err = s.DeleteTask(taskOri.Id)
	assert.Equal(t, store.NotExist, err)

	// verify delete
	task, err = s.GetTask(taskOri.Id)
	assert.Nil(t, task)
	assert.Equal(t, store.NotExist, err)
}

func DoTestStrategy(t *testing.T, s store.Store) {
	strategyOri := &definition.Strategy{
		Id:    "demo-strategy",
		Total: 3,
	}

	// try to fetch not existed task
	strategy, err := s.GetStrategy(strategyOri.Id)
	assert.Nil(t, strategy)
	assert.Equal(t, store.NotExist, err)

	// try to update not existed task
	err = s.UpdateStrategy(strategyOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.NotExist, err)

	// create
	err = s.CreateStrategy(strategyOri)
	assert.Nil(t, err)

	// verify creation
	strategy, err = s.GetStrategy(strategyOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, strategy)
	assert.Equal(t, strategyOri.Id, strategy.Id)

	// recreation
	err = s.CreateStrategy(strategyOri)
	assert.NotNil(t, err)
	assert.Equal(t, store.AlreadyExist, err)

	// task list
	arr, err := s.GetStrategies()
	assert.Nil(t, err)
	assert.NotNil(t, arr)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, strategyOri.Id, arr[0].Id)

	// modify
	strategyOri.Total = 44
	err = s.UpdateStrategy(strategyOri)
	assert.Nil(t, err)

	// verify modify
	strategy, err = s.GetStrategy(strategyOri.Id)
	assert.Nil(t, err)
	assert.NotNil(t, strategy)
	assert.Equal(t, strategyOri.Id, strategy.Id)
	assert.Equal(t, 44, strategy.Total)

	// delete
	err = s.DeleteStrategy(strategyOri.Id)
	assert.Nil(t, err)

	// re-delete
	err = s.DeleteStrategy(strategyOri.Id)
	assert.Equal(t, store.NotExist, err)

	// verify delete
	strategy, err = s.GetStrategy(strategyOri.Id)
	assert.Nil(t, strategy)
	assert.Equal(t, store.NotExist, err)
}

func DoTestScheduler(t *testing.T, s store.Store) {
	schedulerOri := &definition.Scheduler{
		Id: "demo-scheduler",
	}

	list := s.GetSchedulers()
	assert.Empty(t, list)

	s.RegisterScheduler(schedulerOri)

	list = s.GetSchedulers()
	assert.NotEmpty(t, list)

	scheduler, err := s.GetScheduler(schedulerOri.Id)
	assert.Nil(t, err)
	assert.Equal(t, schedulerOri.Id, scheduler.Id)

	scheduler, err = s.GetScheduler("not existed")
	assert.Nil(t, scheduler)
	assert.NotNil(t, err)

	s.UnregisterScheduler(schedulerOri.Id)

	list = s.GetSchedulers()
	assert.Empty(t, list)
}
