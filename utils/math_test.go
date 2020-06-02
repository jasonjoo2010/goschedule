// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

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

func TestMax64(t *testing.T) {
	assert.Equal(t, int64(0), Max64(0, -1))
	assert.Equal(t, int64(0), Max64(-1, 0))

	assert.Equal(t, int64(1), Max64(1, -1))
	assert.Equal(t, int64(1), Max64(-1, 1))

	assert.Equal(t, int64(1), Max64(1, 1))
}

func TestMin(t *testing.T) {
	assert.Equal(t, -1, Min(0, -1))
	assert.Equal(t, -1, Min(-1, 0))

	assert.Equal(t, -1, Min(1, -1))
	assert.Equal(t, -1, Min(-1, 1))

	assert.Equal(t, 1, Min(1, 1))
}

func TestMin64(t *testing.T) {
	assert.Equal(t, int64(-1), Min64(0, -1))
	assert.Equal(t, int64(-1), Min64(-1, 0))

	assert.Equal(t, int64(-1), Min64(1, -1))
	assert.Equal(t, int64(-1), Min64(-1, 1))

	assert.Equal(t, int64(1), Min64(1, 1))
}

func TestAbs(t *testing.T) {
	assert.Equal(t, 1, Abs(-1))
	assert.Equal(t, 1, Abs(1))
	assert.Equal(t, 0, Abs(0))
}

func TestAbs64(t *testing.T) {
	assert.Equal(t, int64(1), Abs64(-1))
	assert.Equal(t, int64(1), Abs64(1))
	assert.Equal(t, int64(0), Abs64(0))
}
