package task_worker

type TaskExecutor interface {
	ExecuteOrWait()
	// ExecuteOrReturn returns false indicating no element in queue
	ExecuteOrReturn() bool
}
