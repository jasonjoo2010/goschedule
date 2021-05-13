package types

import "time"

type ScheduleConfig struct {
	// StallAfterStartup represents the delay after the manager started.
	//	Default to 10 seconds
	StallAfterStartup time.Duration

	// HeartbeatInterval represents the interval of sending a heartbeat.
	//	Default to 5 seconds
	HeartbeatInterval time.Duration

	// DeathTimeout represents the duration no heartbeats happened before we treat the remote manager as dead.
	//	Default to 60 seconds
	DeathTimeout time.Duration

	// ScheduleInterval represents the interval the manager will do its work to guanrantee:
	//	- Appropriate workers are running
	//	- Kick dead managers
	//	Default to 10 seconds
	ScheduleInterval time.Duration

	// ShutdownTimeout indicates whether to wait a maxmum time when closing
	//	Default to wait with no limitation
	ShutdownTimeout time.Duration
}
