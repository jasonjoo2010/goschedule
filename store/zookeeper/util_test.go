// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitPath(t *testing.T) {
	assert.Empty(t, splitPath("/"))
	assert.Equal(t, []string{"demo", "a"}, splitPath("/demo/a"))
	assert.Equal(t, []string{"demo", "a"}, splitPath("/////demo////a/////"))
}
