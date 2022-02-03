package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrace(t *testing.T) {
	data := StackTraceData()
	ptr := fmt.Sprintf("%p", data)
	assert.True(t, data.Len() > 0)
	data.Recycle()

	data1 := StackTraceData()
	ptr1 := fmt.Sprintf("%p", data1)
	assert.True(t, data1.Len() > 0)
	data1.Recycle()

	data2 := StackTraceData()
	ptr2 := fmt.Sprintf("%p", data2)
	assert.True(t, data2.Len() > 0)

	data3 := StackTraceData()
	ptr3 := fmt.Sprintf("%p", data3)
	assert.True(t, data3.Len() > 0)
	data2.Recycle()
	data3.Recycle()

	assert.Equal(t, ptr, ptr1)
	assert.NotEqual(t, ptr2, ptr3)
}
