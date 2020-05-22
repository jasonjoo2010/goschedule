package task_worker

type TaskExecutor interface {
	ExecuteOrWait()
	ExecuteOrReturn()
}
