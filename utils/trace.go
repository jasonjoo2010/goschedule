package utils

import (
	"runtime"
	"sync"
)

type TraceData struct {
	data []byte
	size int
}

func (data *TraceData) Len() int {
	return data.size
}

func (data *TraceData) Data() []byte {
	return data.data[:data.size]
}

func (data *TraceData) String() string {
	return string(data.Data())
}

// call this to reuse the memory
func (data *TraceData) Recycle() {
	tracePool.Put(data)
}

var tracePool = &sync.Pool{
	New: func() interface{} {
		return &TraceData{
			data: make([]byte, 1024*64),
			size: 0,
		}
	},
}

func StackTraceData() *TraceData {
	data := tracePool.Get().(*TraceData)
	data.size = runtime.Stack(data.data, false)
	return data
}
