package task_worker

type TaskModel interface {
	LoopOnce()
	Stop() // called when worker stopping
}
