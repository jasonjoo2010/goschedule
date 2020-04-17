package definition

import (
	"fmt"
	"sync/atomic"
)

type Statistics struct {
	FetchCount     uint64
	FetchDataCount uint64
	DealSuccess    uint64
	DealFail       uint64
	DealTime       uint64 // Millis
	CompareCount   uint64
}

func (s *Statistics) AddFetch(itemCount uint64) {
	atomic.AddUint64(&s.FetchCount, 1)
	if itemCount > 0 {
		atomic.AddUint64(&s.FetchDataCount, itemCount)
	}
}

func (s *Statistics) AddDealing(succ bool, costMillis uint64) {
	if succ {
		atomic.AddUint64(&s.DealSuccess, 1)
	} else {
		atomic.AddUint64(&s.DealFail, 1)
	}
	atomic.AddUint64(&s.DealTime, costMillis)
}

func (s *Statistics) AddCompare() {
	atomic.AddUint64(&s.CompareCount, 1)
}

func (s *Statistics) String() string {
	return fmt.Sprint(
		"FetchCount=", s.FetchCount,
		",FetchDataCount=", s.FetchDataCount,
		",DealSuccess=", s.DealSuccess,
		",DealFail=", s.DealFail,
		",DealTime=", s.DealTime,
		",CompareCount=", s.CompareCount,
	)
}
