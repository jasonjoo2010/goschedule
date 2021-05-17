package types

import "github.com/jasonjoo2010/goschedule/definition"

// TaskBase defines the task used in scheduling.
type TaskBase interface {
	// Select returns tasks to be dealed later.
	//	It will be guaranteed in serial model.
	//	parameter, items, eachFetchNum are from definition of task
	//	ownSign is from name of strategy bond in the form of 'name$ownsign'
	//	It's a kind of relation to strategy but generally task doesn't care about strategy in user's view.
	Select(parameter, ownSign string, items []definition.TaskItem, eachFetchNum int) []interface{}
}

// TaskSingle represents one task one time(routine) model
type TaskSingle interface {
	TaskBase
	// return true if succ false otherwise, but things will still go on
	Execute(task interface{}, ownSign string) bool
}

// TaskBatch represents multiple tasks one time(routine) model
type TaskBatch interface {
	TaskBase
	// return true if succ false otherwise, but things will still go on
	Execute(tasks []interface{}, ownSign string) bool
}

type TaskComparable interface {
	Less(a, b interface{}) bool
}
