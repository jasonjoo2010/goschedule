package utils

import (
	"testing"

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
