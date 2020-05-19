package task_worker

type TaskExecutor interface {
	ExecuteAndWaitWhenEmpty()
}
