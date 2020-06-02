// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateUUIDPrefix(t *testing.T) {
	pre1 := GenerateUUIDPrefix()
	pre2 := GenerateUUIDPrefix()
	assert.NotNil(t, pre1)
	assert.NotNil(t, pre2)
	assert.NotEqual(t, pre1, pre2)
	assert.True(t, len(pre1) > 32)
	assert.True(t, len(pre2) > 32)
}

func TestGenerateUUID(t *testing.T) {
	uuid := GenerateUUID(11)
	assert.NotNil(t, uuid)
	assert.NotEmpty(t, uuid)
	assert.Equal(t, "0000000011", uuid[len(uuid)-10:])
}
