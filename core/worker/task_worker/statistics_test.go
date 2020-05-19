package task_worker

import (
	"testing"
	"time"

	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/stretchr/testify/assert"
)

func TestStatistics(t *testing.T) {
	stat := Statistics{}
	stat.Select(13)
	stat.Execute(true, 20)
	stat.Execute(true, 30)
	stat.Execute(false, 10)
	now := time.Now().Unix() * 1000
	assert.True(t, utils.Abs64(now-stat.LastFetchTime) < 2000)
	assert.Equal(t, int64(1), stat.SelectCount)
	assert.Equal(t, int64(13), stat.SelectItemCount)
	assert.Equal(t, int64(2), stat.ExecuteSuccCount)
	assert.Equal(t, int64(1), stat.ExecuteFailCount)
	assert.Equal(t, int64(60), stat.ExecuteSpendTime)
}
