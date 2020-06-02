// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetHostIPv4(t *testing.T) {
	assert.NotEmpty(t, GetHostIPv4())
}

func TestGetHostName(t *testing.T) {
	assert.NotEmpty(t, GetHostName())
}
