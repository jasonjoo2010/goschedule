package task_worker

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/store"
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
	parameter      string
	ownSign        string
	strategyDefine definition.Strategy
	taskDefine     definition.Task
	taskItems      []definition.TaskItem
	configVersion  int64
	noItemsCycles  int
	store          store.Store
	runtime        definition.TaskRuntime
	notifierC      chan int
	data           chan interface{}
	queuedData     []interface{}
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

func getTaskFromType(t reflect.Type) TaskBase {
	if v, ok := reflect.New(t).Interface().(TaskBase); ok {
		return v
	}
	logrus.Warn("Entry registered is not a convertable type: ", t)
	return nil
}

func getTask(name string) TaskBase {
	var (
		ok bool
		v  interface{}
	)
	if v, ok = taskRegistryMap.Load(name); !ok {
		logrus.Warn("No task type or inst registered for key: ", name)
		return nil
	}
	t, ok := v.(reflect.Type)
	if ok {
		return getTaskFromType(t)
	}
	val, ok := v.(TaskBase)
	if ok {
		return val
	}
	logrus.Warn("Entry registered for key: ", name, " is not either a type nor inst")
	return nil
}

// RegisterTaskType registers a task type with key inferred by its type
func RegisterTaskType(task TaskBase) {
	if task == nil {
		panic("Could not register a task using nil as value")
	}
	RegisterTaskTypeName(utils.TypeName(utils.Dereference(task)), task)
}

// RegisterTaskTypeName registers a task type with key
func RegisterTaskTypeName(name string, task TaskBase) {
	if name == "" {
		panic("Could not register a task using empty name")
	}
	if task == nil {
		panic("Could not register a task using nil as value")
	}
	t := reflect.TypeOf(utils.Dereference(task))
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
func NewTask(strategy definition.Strategy, task definition.Task, store store.Store, schedulerId string) (worker.Worker, error) {
	var inst TaskBase
	sequence, err := store.Sequence()
	if err != nil {
		logrus.Error("Generate sequence from storage failed: ", err.Error())
		return nil, errors.New("Generate sequence from storage failed: " + err.Error())
	}
	inst = getTask(task.Bind)
	if inst == nil {
		logrus.Warn("Create task worker failed: ", task.Bind)
		return nil, errors.New("Convert to TaskBase failed: " + task.Bind)
	}
	logrus.Info("New task ", task.Id, " created")
	w := &TaskWorker{
		notifierC:       make(chan int),
		data:            make(chan interface{}, utils.Max(10, task.FetchCount*len(task.Items)*2)),
		TimeoutShutdown: 10 * time.Second,
		task:            inst,
		strategyDefine:  strategy,
		ownSign:         utils.OwnSign(strategy.Id),
		taskDefine:      task,
		taskItems:       make([]definition.TaskItem, 0),
		parameter:       task.Parameter,
		store:           store,
		runtime: definition.TaskRuntime{
			Id:            utils.GenerateUUID(sequence),
			Version:       1,
			CreateTime:    time.Now().Unix() * 1000,
			LastHeartBeat: time.Now().Unix() * 1000,
			Hostname:      utils.GetHostName(),
			Ip:            utils.GetHostIPv4(),
			ExecutorCount: task.ExecutorCount,
			SchedulerId:   schedulerId,
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
	logrus.Info("Create a task worker, cronStart=", w.schedStart, ", cronEnd=", w.schedEnd, ", interval=", w.interval/time.Millisecond)
	return w, nil
}

func (w *TaskWorker) NeedStop() bool {
	return w.needStop
}

// shouldRun returns false when it cannot decide the result
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

func (w *TaskWorker) fillOrQueued(arr []interface{}) {
	for i, obj := range arr {
		select {
		case w.data <- obj:
		default:
			// should not happen
			w.queuedData = append(w.queuedData, arr[i:]...)
			return
		}
	}
}

func (w *TaskWorker) selectOnce() {
	// lock to block other routine from select concurrently (Especially in stream model)
	w.Lock()
	defer w.Unlock()
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("Selecting error: ", r)
		}
	}()
	if len(w.queuedData) > 0 {
		arr := w.queuedData
		w.queuedData = nil
		w.fillOrQueued(arr)
		return
	}
	ver, err := w.store.GetTaskItemsConfigVersion(w.strategyDefine.Id, w.taskDefine.Id)
	if err == nil && w.configVersion < ver {
		// make sure no queued items
		maxWait := time.Millisecond * 500
		for len(w.data) > 0 && maxWait > 0 {
			time.Sleep(10 * time.Millisecond)
			maxWait -= 10 * time.Millisecond
		}
		if len(w.data) > 0 {
			logrus.Info("Queue is not empty and wait to reload next time")
			return
		}
		w.reloadTaskItems()
		w.configVersion = ver
	}
	// Check available task item
	if len(w.taskItems) < 1 {
		w.noItemsCycles++
		if w.noItemsCycles >= 10 {
			logrus.Warn("Cannot get any task item after quite a long time.")
			w.noItemsCycles = 0
		}
		utils.Delay(w, time.Duration(w.taskDefine.HeartbeatInterval)*time.Millisecond)
		return
	}
	w.noItemsCycles = 0
	arr := w.task.Select(w.parameter, w.ownSign, w.taskItems, w.taskDefine.FetchCount)
	arr_size := len(arr)
	w.Statistics.Select(int64(arr_size))
	if arr_size < 1 {
		w.inCron = false
		if w.intervalNoData > 0 {
			utils.Delay(w, w.intervalNoData)
		} else if w.interval > 0 {
			utils.Delay(w, w.interval)
		}
		return
	}
	w.fillOrQueued(arr)
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
		for len(w.data) > 0 || len(w.queuedData) > 0 {
			w.executor.ExecuteOrReturn()
			if len(w.data) == 0 && len(w.queuedData) > 0 {
				w.queuedData = nil
				arr := w.queuedData
				w.fillOrQueued(arr)
			}
		}
		// wait executors
		for w.executors > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		w.notifierC <- 1
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
		case v := <-w.notifierC:
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
	w.started = false
	w.needStop = false
	logrus.Info("Worker of strategy ", strategyId, " stopped")
}
