// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package definition

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatistics(t *testing.T) {
	stat := Statistics{}
	stat.Select(13)
	stat.Execute(true, 20)
	stat.Execute(true, 30)
	stat.Execute(false, 10)
	now := time.Now().Unix() * 1000
	assert.True(t, now-stat.LastFetchTime < 2000)
	assert.Equal(t, int64(1), stat.SelectCount)
	assert.Equal(t, int64(13), stat.SelectItemCount)
	assert.Equal(t, int64(2), stat.ExecuteSuccCount)
	assert.Equal(t, int64(1), stat.ExecuteFailCount)
	assert.Equal(t, int64(60), stat.ExecuteSpendTime)
}
