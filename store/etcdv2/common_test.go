// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	"testing"

	"github.com/jasonjoo2010/goschedule/store"
	"github.com/stretchr/testify/assert"
)

func TestCommon(t *testing.T) {
	s := newStorage()

	assert.False(t, s.exists("/test"))
	assert.False(t, s.exists("/test/a"))

	assert.Equal(t, store.NotExist, s.update("/test/a", "", true))
	assert.Nil(t, s.create("/test/a", ""))
	assert.True(t, s.exists("/test"))
	assert.True(t, s.exists("/test/a"))
	assert.Equal(t, store.AlreadyExist, s.create("/test/a", ""))
	assert.Nil(t, s.update("/test/a", "", true))

	assert.Nil(t, s.remove("/test/a", false))
	assert.True(t, s.exists("/test"))
	assert.False(t, s.exists("/test/a"))

	assert.Nil(t, s.create("/test/a", "nodeA"))
	assert.True(t, s.exists("/test"))
	assert.True(t, s.exists("/test/a"))

	list, _ := s.getChildren("/test", false)
	assert.Equal(t, 1, len(list))

	assert.Nil(t, s.remove("/test", true))
	assert.False(t, s.exists("/test"))
	assert.False(t, s.exists("/test/a"))
}
