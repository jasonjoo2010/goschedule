package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store/memory"
	"github.com/stretchr/testify/assert"
)

func TestLeader(t *testing.T) {
	store := memory.New()
	manager1 := newManager(t, store)
	manager2 := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		Id:     "s0",
		IpList: []string{"localhost"},
	})

	assert.False(t, manager1.isLeader("s0"))
	assert.False(t, manager2.isLeader("s0"))

	manager1.generateRuntimes()

	assert.True(t, manager1.isLeader("s0"))
	assert.False(t, manager2.isLeader("s0"))

	manager2.generateRuntimes()

	isLeader1 := manager1.isLeader("s0")
	isLeader2 := manager2.isLeader("s0")
	assert.False(t, isLeader1 && isLeader2)
	assert.True(t, isLeader1 || isLeader2)
}

func TestExpiredSchedulers(t *testing.T) {
	store := memory.New()
	manager := newManager(t, store)
	managerExpired := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		Id:     "s0",
		IpList: []string{"localhost"},
	})

	manager.Start()

	time.Sleep(200 * time.Millisecond)
	list, _ := store.GetSchedulers()
	assert.Equal(t, 1, len(list))

	// Register an expired one
	managerExpired.registerInfo()
	managerExpired.generateRuntimes()
	list, _ = store.GetSchedulers()
	assert.Equal(t, 2, len(list))

	runtimes, _ := store.GetStrategyRuntimes("s0")
	assert.Equal(t, 2, len(runtimes))
	fmt.Println(runtimes)

	// Wait to cleared
	time.Sleep(manager.DeathTimeout)

	list, _ = store.GetSchedulers()
	assert.Equal(t, 1, len(list))
	fmt.Println(list)

	runtimes, _ = store.GetStrategyRuntimes("s0")
	assert.Equal(t, 1, len(runtimes))
	fmt.Println(runtimes)

	manager.Shutdown()
}

func TestGenerateRuntimes(t *testing.T) {
	store := memory.New()
	manager := newManager(t, store)

	store.CreateStrategy(&definition.Strategy{
		Id:     "s0",
		IpList: []string{"localhost"},
	})
	store.CreateStrategy(&definition.Strategy{
		Id:     "s1",
		IpList: []string{"127.0.0.1"},
	})

	list, _ := store.GetStrategyRuntimes("s0")
	assert.Equal(t, 0, len(list))
	list, _ = store.GetStrategyRuntimes("s1")
	assert.Equal(t, 0, len(list))

	manager.generateRuntimes()
	list, _ = store.GetStrategyRuntimes("s0")
	assert.Equal(t, 1, len(list))
	list, _ = store.GetStrategyRuntimes("s1")
	assert.Equal(t, 1, len(list))
}

func TestAssign(t *testing.T) {
	store := memory.New()
	manager1 := newManager(t, store)
	manager2 := newManager(t, store)
	manager3 := newManager(t, store)
	manager1.ScheduleInterval = 500 * time.Millisecond
	manager2.ScheduleInterval = 500 * time.Millisecond
	manager3.ScheduleInterval = 500 * time.Millisecond

	TASK_COUNT := 2

	store.CreateStrategy(&definition.Strategy{
		Id:                   "S",
		IpList:               []string{"127.0.0.1"},
		Total:                TASK_COUNT,
		Kind:                 definition.SimpleKind,
		MaxOnSingleScheduler: 1,
		Enabled:              true,
	})

	manager1.Start()
	manager2.Start()
	manager3.Start()

	time.Sleep(2 * time.Second)
	runtimes, _ := store.GetStrategyRuntimes("S")
	total := 0
	for _, r := range runtimes {
		total += r.Num
	}
	assert.Equal(t, TASK_COUNT, total)

	manager1.Shutdown()
	manager2.Shutdown()
	manager3.Shutdown()
}
