// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommon(t *testing.T) {
	s := newStorage()

	assert.False(t, s.exists("/test"))
	assert.False(t, s.exists("/test/a"))

	assert.Nil(t, s.createPath("/test/a", true))
	assert.True(t, s.exists("/test"))
	assert.True(t, s.exists("/test/a"))

	assert.Nil(t, s.removePath("/test/a", false))
	assert.True(t, s.exists("/test"))
	assert.False(t, s.exists("/test/a"))

	assert.Nil(t, s.createPath("/test/a", true))
	assert.True(t, s.exists("/test"))
	assert.True(t, s.exists("/test/a"))

	list := s.getChildren("/test", true)
	assert.Equal(t, 1, len(list))

	assert.Nil(t, s.removePath("/test", true))
	assert.False(t, s.exists("/test"))
	assert.False(t, s.exists("/test/a"))
}
