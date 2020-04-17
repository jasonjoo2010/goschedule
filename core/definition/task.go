package definition

import "encoding/json"

// Sleep Model:
//	t0        t1        t2        t3
//	fetch()   *         *         *
//  exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	done      done      exe()     dont
//  done      done      fetch()   done
//	exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	exe()     done      done      done
//	fetch()   done      done      done
//	.......

// NoSleep Model:
//	t0        t1        t2        t3
//	fetch()   *         *         *
//  exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	done      exe()     exe()     exe()
//	fetch()   exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	exe()     exe()     exe()     exe()
//	.......

type Model int

const (
	_ Model = iota
	Sleep
	NoSleep
)

type Task struct {
	Id              string
	HeartbeatTime   int // In millis
	LifeTime        int // Timeout to be treated dead
	DelayWhenNoData int // Whether to delay specified time (millis) if no data selected
	DelayWithData   int // Whether to delay specified time (millis) if something are selected
	FetchCount      int
	ExecuteCount    int
	Threads         int
	Model           Model
	Parameter       string
	BindName        string // Binded to registry
	Items           []*TaskItem
	MaxItems        int // max task items per Worker

	// format  0     *     *     *     *     ?
	//         sec   min   hour  day   month week
	CronBegin string
	CronEnd   string
}

func (t *Task) String() string {
	data, _ := json.Marshal(t)
	return string(data)
}
