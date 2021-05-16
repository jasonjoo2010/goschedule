// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package definition

import "fmt"

type Scheduler struct {
	ID            string
	LastHeartbeat int64
	Enabled       bool // Whether it should begin to schedule
}

func (s *Scheduler) String() string {
	return fmt.Sprint("{id=", s.ID, ",lastHeartbeat=", s.LastHeartbeat, ",enabled=", s.Enabled, "}")
}
