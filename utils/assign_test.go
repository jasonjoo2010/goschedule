package utils

import (
	"testing"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/stretchr/testify/assert"
)

func TestAssignWorkers(t *testing.T) {
	assert.Equal(t, []int{10}, AssignWorkers(1, 10, 0))
	assert.Equal(t, []int{5, 5}, AssignWorkers(2, 10, 0))
	assert.Equal(t, []int{4, 3, 3}, AssignWorkers(3, 10, 0))
	assert.Equal(t, []int{3, 3, 2, 2}, AssignWorkers(4, 10, 0))
	assert.Equal(t, []int{2, 2, 2, 2, 2}, AssignWorkers(5, 10, 0))
	assert.Equal(t, []int{2, 2, 2, 2, 1, 1}, AssignWorkers(6, 10, 0))
	assert.Equal(t, []int{2, 2, 2, 1, 1, 1, 1}, AssignWorkers(7, 10, 0))
	assert.Equal(t, []int{2, 2, 1, 1, 1, 1, 1, 1}, AssignWorkers(8, 10, 0))
	assert.Equal(t, []int{2, 1, 1, 1, 1, 1, 1, 1, 1}, AssignWorkers(9, 10, 0))
	assert.Equal(t, []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, AssignWorkers(10, 10, 0))
	assert.Equal(t, []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}, AssignWorkers(10, 1, 0))
	assert.Equal(t, []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}, AssignWorkers(10, 1, 0))
	assert.Equal(t, []int{3}, AssignWorkers(1, 10, 3))
	assert.Equal(t, []int{3, 3}, AssignWorkers(2, 10, 3))
	assert.Equal(t, []int{3, 3, 3}, AssignWorkers(3, 10, 3))
	assert.Equal(t, []int{3, 3, 2, 2}, AssignWorkers(4, 10, 3))
	assert.Equal(t, []int{2, 2, 2, 2, 2}, AssignWorkers(5, 10, 3))
	assert.Equal(t, []int{2, 2, 2, 2, 1, 1}, AssignWorkers(6, 10, 3))
	assert.Equal(t, []int{2, 2, 2, 1, 1, 1, 1}, AssignWorkers(7, 10, 3))
	assert.Equal(t, []int{2, 2, 1, 1, 1, 1, 1, 1}, AssignWorkers(8, 10, 3))
	assert.Equal(t, []int{2, 1, 1, 1, 1, 1, 1, 1, 1}, AssignWorkers(9, 10, 3))
	assert.Equal(t, []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, AssignWorkers(10, 10, 3))
}

func TestCanSchedule(t *testing.T) {
	assert.True(t, CanSchedule([]string{"127.0.0.1"}, "", "192.168.123.1"))
	assert.True(t, CanSchedule([]string{"localhost"}, "", "192.168.123.1"))
	assert.True(t, CanSchedule([]string{"127.0.0.1"}, "demo", ""))
	assert.True(t, CanSchedule([]string{"localhost"}, "demo", ""))

	assert.False(t, CanSchedule([]string{}, "", "192.168.123.1"))
	assert.False(t, CanSchedule([]string{}, "demo", ""))

	assert.False(t, CanSchedule([]string{"demo1", "demo2"}, "demo", ""))
	assert.False(t, CanSchedule([]string{"demo1", "demo2"}, "demo", "192.168.0.1"))
	assert.True(t, CanSchedule([]string{"demo1", "demo2"}, "demo1", "192.168.0.1"))
	assert.True(t, CanSchedule([]string{"demo1", "demo2", "192.168.0.1"}, "demo", "192.168.0.1"))
}

func TestSortRuntimesWithShuffle(t *testing.T) {
	runtimes := make([]*definition.StrategyRuntime, 5)
	runtimes[0] = &definition.StrategyRuntime{
		SchedulerId:  "A",
		StrategyId:   "S",
		RequestedNum: 0,
	}
	runtimes[1] = &definition.StrategyRuntime{
		SchedulerId:  "B",
		StrategyId:   "S",
		RequestedNum: 2,
	}
	runtimes[2] = &definition.StrategyRuntime{
		SchedulerId:  "C",
		StrategyId:   "S",
		RequestedNum: 0,
	}
	runtimes[3] = &definition.StrategyRuntime{
		SchedulerId:  "D",
		StrategyId:   "S",
		RequestedNum: 2,
	}
	runtimes[4] = &definition.StrategyRuntime{
		SchedulerId:  "E",
		StrategyId:   "S",
		RequestedNum: 0,
	}

	SortRuntimesWithShuffle(runtimes)

	for i := 1; i < len(runtimes); i++ {
		assert.GreaterOrEqual(t, runtimes[i-1].RequestedNum, runtimes[i].RequestedNum)
	}
}

func TestSortStrategyRuntimes(t *testing.T) {
	runtimes := make([]*definition.StrategyRuntime, 4)
	runtimes[0] = &definition.StrategyRuntime{
		SchedulerId: "a$b$3$00000012",
	}
	runtimes[1] = &definition.StrategyRuntime{
		SchedulerId: "c$b$3$00000011",
	}
	runtimes[2] = &definition.StrategyRuntime{
		SchedulerId: "b$b$3$00000013",
	}
	runtimes[3] = &definition.StrategyRuntime{
		SchedulerId: "a$b$3$000009",
	}

	SortStrategyRuntimes(runtimes)

	assert.Equal(t, "a$b$3$000009", runtimes[0].SchedulerId)
	assert.Equal(t, "b$b$3$00000013", runtimes[3].SchedulerId)
}

func TestSortSchedulers(t *testing.T) {
	schedulers := make([]*definition.Scheduler, 4)
	schedulers[0] = &definition.Scheduler{
		Id: "a$b$3$00000012",
	}
	schedulers[1] = &definition.Scheduler{
		Id: "c$b$3$00000011",
	}
	schedulers[2] = &definition.Scheduler{
		Id: "b$b$3$00000013",
	}
	schedulers[3] = &definition.Scheduler{
		Id: "a$b$3$000009",
	}

	SortSchedulers(schedulers)

	assert.Equal(t, "a$b$3$000009", schedulers[0].Id)
	assert.Equal(t, "b$b$3$00000013", schedulers[3].Id)
}

func TestSortTaskRuntimes(t *testing.T) {
	runtimes := make([]*definition.TaskRuntime, 4)
	runtimes[0] = &definition.TaskRuntime{
		Id: "a$b$3$00000012",
	}
	runtimes[1] = &definition.TaskRuntime{
		Id: "c$b$3$00000011",
	}
	runtimes[2] = &definition.TaskRuntime{
		Id: "b$b$3$00000013",
	}
	runtimes[3] = &definition.TaskRuntime{
		Id: "a$b$3$000009",
	}

	SortTaskRuntimes(runtimes)

	assert.Equal(t, "a$b$3$000009", runtimes[0].Id)
	assert.Equal(t, "b$b$3$00000013", runtimes[3].Id)
}

func TestSortTaskAssignments(t *testing.T) {
	assignments := make([]*definition.TaskAssignment, 4)
	assignments[0] = &definition.TaskAssignment{
		ItemId: "a",
	}
	assignments[1] = &definition.TaskAssignment{
		ItemId: "c",
	}
	assignments[2] = &definition.TaskAssignment{
		ItemId: "bb",
	}
	assignments[3] = &definition.TaskAssignment{
		ItemId: "b",
	}

	SortTaskAssignments(assignments)

	assert.Equal(t, "a", assignments[0].ItemId)
	assert.Equal(t, "bb", assignments[2].ItemId)
	assert.Equal(t, "c", assignments[3].ItemId)
}

func TestContainsTaskItem(t *testing.T) {
	arr := make([]definition.TaskItem, 0, 10)
	arr = append(arr, definition.TaskItem{
		Id: "item0",
	}, definition.TaskItem{
		Id: "item1",
	}, definition.TaskItem{
		Id: "item2",
	}, definition.TaskItem{
		Id: "item3",
	}, definition.TaskItem{
		Id: "item4",
	})

	assert.True(t, ContainsTaskItem(arr, "item0"))
	assert.True(t, ContainsTaskItem(arr, "item1"))
	assert.True(t, ContainsTaskItem(arr, "item2"))
	assert.True(t, ContainsTaskItem(arr, "item4"))
	assert.False(t, ContainsTaskItem(arr, "item5"))
}

func TestRemoveTaskItem(t *testing.T) {
	arr := make([]definition.TaskItem, 0, 10)
	arr = append(arr, definition.TaskItem{
		Id: "item0",
	}, definition.TaskItem{
		Id: "item1",
	}, definition.TaskItem{
		Id: "item2",
	}, definition.TaskItem{
		Id: "item3",
	}, definition.TaskItem{
		Id: "item4",
	})

	arr = RemoveTaskItem(arr, "item0")
	assert.Equal(t, 4, len(arr))

	arr = RemoveTaskItem(arr, "item2")
	assert.Equal(t, 3, len(arr))

	arr = RemoveTaskItem(arr, "item4")
	assert.Equal(t, 2, len(arr))
}
