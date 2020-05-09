package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	assert.Equal(t, 0, Max(0, -1))
	assert.Equal(t, 0, Max(-1, 0))

	assert.Equal(t, 1, Max(1, -1))
	assert.Equal(t, 1, Max(-1, 1))

	assert.Equal(t, 1, Max(1, 1))
}

func TestMin(t *testing.T) {
	assert.Equal(t, -1, Min(0, -1))
	assert.Equal(t, -1, Min(-1, 0))

	assert.Equal(t, -1, Min(1, -1))
	assert.Equal(t, -1, Min(-1, 1))

	assert.Equal(t, 1, Min(1, 1))
}

func TestAbs(t *testing.T) {
	assert.Equal(t, 1, Abs(-1))
	assert.Equal(t, 1, Abs(1))
	assert.Equal(t, 0, Abs(0))
}
