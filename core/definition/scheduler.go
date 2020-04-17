package definition

import "fmt"

type Scheduler struct {
	Id            string
	LastHeartbeat int64
	Enabled       bool // Whether it should begin to schedule
}

func (s *Scheduler) String() string {
	return fmt.Sprint("{id=", s.Id, ",lastHeartbeat=", s.LastHeartbeat, ",enabled=", s.Enabled, "}")
}
