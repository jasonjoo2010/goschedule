package task_worker

type TaskExecutor interface {
	// ExecuteOrReturn returns false indicating no element in queue
	ExecuteOrReturn() bool
}
