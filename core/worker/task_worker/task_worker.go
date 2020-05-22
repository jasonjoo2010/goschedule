package task_worker

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jasonjoo2010/goschedule/core"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var (
	taskRegistryMap sync.Map
)

// TaskBase defines the task used in scheduling.
type TaskBase interface {
	// Select returns tasks to be dealed later.
	//	It will be guaranteed in serial model.
	//	parameter, items, eachFetchNum are from definition of task
	//	ownSign is from name of strategy binded in the form of 'name$ownsign'
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

// TaskWorker implements a task-driven worker.
//	Strategy.Bind should be the identifier of task(on console panel).
type TaskWorker struct {
	sync.Mutex
	strategyId     string
	parameter      string
	ownSign        string
	strategyDefine definition.Strategy
	taskDefine     definition.Task
	taskItems      []definition.TaskItem
	manager        *core.ScheduleManager
	runtime        definition.TaskRuntime
	notifier       chan int
	data           chan interface{}
	model          TaskModel
	executor       TaskExecutor
	task           TaskBase
	executors      int32
	schedStart     cron.Schedule
	schedEnd       cron.Schedule
	interval       time.Duration
	intervalNoData time.Duration
	inCron         bool // Flagged indicating schedStart was triggered
	started        bool
	needStop       bool

	// statistics
	NextBeginTime int64
	Statistics    definition.Statistics

	// TimeoutShutdown is the timeout when waiting to close the worker
	TimeoutShutdown time.Duration
}

func GetTaskType(name string) reflect.Type {
	if v, ok := taskRegistryMap.Load(name); ok {
		t, ok := v.(reflect.Type)
		if ok {
			return t
		}
		logrus.Warn("Task type registered for key: ", name, " is not a reflect.Type")
		return nil
	}
	logrus.Warn("No task type registered for key: ", name)
	return nil
}

func GetTaskInst(name string) TaskBase {
	if v, ok := taskRegistryMap.Load(name); ok {
		t, ok := v.(TaskBase)
		if ok {
			return t
		}
		logrus.Warn("Task instance registered for key: ", name, " is not in correct type")
		return nil
	}
	logrus.Warn("No task instance registered for key: ", name)
	return nil
}

// RegisterTaskType registers a task type with key inferred by its type
func RegisterTaskType(task TaskBase) {
	RegisterTaskTypeName(utils.TypeName(task), task)
}

// RegisterTaskTypeName registers a task type with key
func RegisterTaskTypeName(name string, task TaskBase) {
	t := reflect.TypeOf(task)
	taskRegistryMap.Store(name, t)
	logrus.Info("Register new task type: ", name)
}

// RegisterTaskInst registers a task in single instance model with key inferred by its type
func RegisterTaskInst(task TaskBase) {
	RegisterTaskInstName(utils.TypeName(task), task)
}

// RegisterTaskInstName registers a task in single instance model with given key
func RegisterTaskInstName(name string, task TaskBase) {
	taskRegistryMap.Store(name, task)
	logrus.Info("Register a task instance: ", name)
}

// NewTask creates a new task and initials necessary fields
//	Please don't initial TaskWorker manually
func NewTask(strategy definition.Strategy, task definition.Task, single bool, manager *core.ScheduleManager) (worker.Worker, error) {
	var inst TaskBase
	sequence, err := manager.Store().Sequence()
	if err != nil {
		logrus.Error("Generate sequence from storage failed: ", err.Error())
		return nil, errors.New("Generate sequence from storage failed: " + err.Error())
	}
	if single {
		inst = GetTaskInst(task.Bind)
		if inst == nil {
			logrus.Warn("Fetch task worker instance failed for ", task.Bind)
			return nil, errors.New("No specific task instance found: " + task.Bind)
		}
	} else {
		t := GetTaskType(task.Bind)
		if t == nil {
			logrus.Warn("Create task worker failed: ", task.Bind, " cannot be located")
			return nil, errors.New("No specific task type found: " + task.Bind)
		}
		var ok bool
		inst, ok = reflect.New(t).Elem().Interface().(TaskBase)
		if !ok {
			logrus.Warn("Create task worker failed: ", task.Bind, " cannot be converted to TaskBase")
			return nil, errors.New("Convert to TaskBase failed: " + task.Bind)
		}
		logrus.Info("New task instance of task ", task.Id, " created")
	}
	w := &TaskWorker{
		notifier:        make(chan int),
		data:            make(chan interface{}, utils.Max(10, task.FetchCount*2)),
		TimeoutShutdown: 10 * time.Second,
		task:            inst,
		strategyDefine:  strategy,
		taskDefine:      task,
		taskItems:       make([]definition.TaskItem, 0),
		parameter:       task.Parameter,
		manager:         manager,
		runtime: definition.TaskRuntime{
			Id:            utils.GenerateUUID(sequence),
			Version:       1,
			CreateTime:    time.Now().Unix() * 1000,
			LastHeartBeat: time.Now().Unix() * 1000,
			Hostname:      utils.GetHostName(),
			Ip:            utils.GetHostIPv4(),
			ExecutorCount: task.ExecutorCount,
			SchedulerId:   manager.Scheduler().Id,
			StrategyId:    strategy.Id,
			OwnSign:       utils.OwnSign(strategy.Id),
			TaskId:        task.Id,
			Bind:          task.Bind,
		},
	}
	w.schedStart, w.schedEnd = utils.ParseStrategyCron(&strategy)
	if task.Interval > 0 {
		w.interval = time.Duration(task.Interval) * time.Millisecond
	}
	if task.IntervalNoData > 0 {
		w.intervalNoData = time.Duration(task.IntervalNoData) * time.Millisecond
	}
	if task.Model == definition.Stream {
		w.model = NewStreamModel(w)
	} else {
		w.model = NewNormalModel(w)
	}
	if w.taskDefine.BatchCount > 1 {
		t, ok := inst.(TaskBatch)
		if !ok {
			return nil, errors.New("Specific bind is not a TaskBatch: " + task.Bind)
		}
		w.executor = &BatchExecutor{
			worker: w,
			task:   t,
			pool: sync.Pool{
				New: func() interface{} {
					return make([]interface{}, 0, w.taskDefine.BatchCount)
				},
			},
		}
	} else {
		t, ok := inst.(TaskSingle)
		if !ok {
			return nil, errors.New("Specific bind is not a TaskSingle: " + task.Bind)
		}
		w.executor = &SingleExecutor{
			worker: w,
			task:   t,
		}
	}
	logrus.Info("Create a task worker, cronStart=", w.schedStart, ", cronEnd=", w.schedEnd, ", interval=", w.interval/time.Millisecond, "ms")
	return w, nil
}

func (w *TaskWorker) NeedStop() bool {
	return w.needStop
}

func (w *TaskWorker) shouldRun() bool {
	if w.schedStart == nil {
		// no cron settings
		return true
	}
	if w.schedEnd == nil {
		// continue until nothing selected
		return w.inCron
	}
	// Cannot decide
	return false
}

func (w *TaskWorker) executeOnceOrWait() {
	w.executor.ExecuteOrWait()
}

// inner loop, select() -> execute()
func (w *TaskWorker) selectOnce() {
	if w.needStop {
		return
	}
	// lock to block other routine from select concurrently (Especially in stream model)
	w.Lock()
	defer w.Unlock()
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("Selecting error: ", r)
		}
	}()
	if w.manager.Store().ShouldTaskReloadItems(w.taskDefine.Id, w.runtime.Id) {
		// make sure no queued items
		for len(w.data) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		w.reloadTaskItems()
	}
	arr := w.task.Select(w.parameter, w.ownSign, w.taskItems, w.taskDefine.FetchCount)
	arr_size := len(arr)
	w.Statistics.Select(int64(arr_size))
	if arr_size < 1 {
		w.inCron = false
		if w.intervalNoData > 0 {
			utils.Delay(w, w.intervalNoData)
		}
		return
	}
	for _, obj := range arr {
		w.data <- obj
	}
	if w.interval > 0 {
		utils.Delay(w, w.interval)
	}
}

func (w *TaskWorker) loopOther() {
	atomic.AddInt32(&w.executors, 1)
	defer atomic.AddInt32(&w.executors, -1)
	for {
		// cron
		if !w.shouldRun() {
			utils.CronDelay(w, w.schedStart, w.schedEnd)
			if w.needStop {
				break
			}
		}
		w.model.LoopOnce()
		if w.needStop {
			break
		}
	}
}

// main loop(outer)
func (w *TaskWorker) loopMain() {
	atomic.AddInt32(&w.executors, 1)
	defer func() {
		atomic.AddInt32(&w.executors, -1)
		close(w.data)
		// empty the queue
		for len(w.data) > 0 {
			w.executor.ExecuteOrReturn()
		}
		// wait executors
		for w.executors > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		w.notifier <- 1
		w.started = false
		w.needStop = false
	}()
	// create other executors
	for i := 1; i < w.taskDefine.ExecutorCount; i++ {
		go w.loopOther()
	}
	for {
		// cron
		if !w.shouldRun() {
			utils.CronDelay(w, w.schedStart, w.schedEnd)
			if w.needStop {
				break
			}
		}
		w.model.LoopOnce()
		if w.needStop {
			break
		}
	}
}

func (w *TaskWorker) Start(strategyId, parameter string) {
	w.Lock()
	defer w.Unlock()
	if w.started {
		logrus.Warn("Task Worker has already started, ignore")
		return
	}
	w.started = true
	w.strategyId = strategyId
	w.ownSign = utils.OwnSign(strategyId)
	if parameter != "" {
		w.parameter = parameter
	}
	go w.loopMain()
	go w.heartbeat()
	go w.schedule()
}

func (w *TaskWorker) Stop(strategyId, parameter string) {
	w.Lock()
	defer w.Unlock()
	w.needStop = true
	timeout := time.NewTimer(w.TimeoutShutdown)
	mask := 0
LOOP:
	for {
		select {
		case v := <-w.notifier:
			mask |= v
			if mask == 7 {
				// succ
				timeout.Stop()
				break LOOP
			}
		case <-timeout.C:
			// timeout
			logrus.Error("Failed to stop a FuncWorker")
			break LOOP
		}
	}
	logrus.Error("Worker of strategy ", strategyId, " stopped")
}
