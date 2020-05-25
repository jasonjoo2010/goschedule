package memory

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jasonjoo2010/enhanced-utils/concurrent/distlock"
	lockstore "github.com/jasonjoo2010/enhanced-utils/concurrent/distlock/mock"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/utils"
)

type MemoryStore struct {
	sequence        uint64
	mutex           *sync.Mutex
	lock            *distlock.DistLock
	tasks           map[string]*definition.Task
	strategies      map[string]*definition.Strategy
	schedulers      map[string]*definition.Scheduler
	runtimes        map[runtimeKey]*definition.StrategyRuntime
	taskRuntimes    map[runtimeKey]*definition.TaskRuntime
	taskReloadFlags map[runtimeKey]bool
	taskAssignments map[runtimeKey]*definition.TaskAssignment
}

type runtimeKey struct {
	left, right string
}

func (r *runtimeKey) String() string {
	b := strings.Builder{}
	b.WriteString(r.left)
	b.WriteString("/")
	b.WriteString(r.right)
	return b.String()
}

func New() *MemoryStore {
	return &MemoryStore{
		mutex:           &sync.Mutex{},
		lock:            distlock.NewMutex("", 60*time.Second, lockstore.New()),
		tasks:           make(map[string]*definition.Task),
		strategies:      make(map[string]*definition.Strategy),
		schedulers:      make(map[string]*definition.Scheduler),
		runtimes:        make(map[runtimeKey]*definition.StrategyRuntime),
		taskRuntimes:    make(map[runtimeKey]*definition.TaskRuntime),
		taskAssignments: make(map[runtimeKey]*definition.TaskAssignment),
		taskReloadFlags: make(map[runtimeKey]bool),
	}
}

func (s *MemoryStore) Lock() *distlock.DistLock {
	return s.lock
}

func (s *MemoryStore) Name() string {
	return "memory"
}

func (s *MemoryStore) Time() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (s *MemoryStore) Close() error {
	return nil
}

func (s *MemoryStore) Sequence() (uint64, error) {
	return atomic.AddUint64(&s.sequence, 1), nil
}

//
// Task related
//

func (s *MemoryStore) GetTask(id string) (*definition.Task, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	task, ok := s.tasks[id]
	if ok {
		t := *task
		return &t, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetTasks() ([]*definition.Task, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	list := make([]*definition.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		t := *task
		list = append(list, &t)
	}
	return list, nil
}

func (s *MemoryStore) CreateTask(task *definition.Task) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.tasks[task.Id]; ok {
		return store.AlreadyExist
	}
	t := *task
	s.tasks[task.Id] = &t
	return nil
}

func (s *MemoryStore) UpdateTask(task *definition.Task) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.tasks[task.Id]; !ok {
		return store.NotExist
	}
	t := *task
	s.tasks[task.Id] = &t
	return nil
}

func (s *MemoryStore) RemoveTask(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return store.NotExist
	}
	delete(s.tasks, id)
	return nil
}

//
// task runtimes
//

func (s *MemoryStore) GetTaskRuntime(taskId, id string) (*definition.TaskRuntime, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	t, ok := s.taskRuntimes[runtimeKey{taskId, id}]
	if ok {
		r := *t
		return &r, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetTaskRuntimes(taskId string) ([]*definition.TaskRuntime, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	arr := make([]*definition.TaskRuntime, 0, 1)
	for k, v := range s.taskRuntimes {
		if k.left == taskId {
			r := *v
			arr = append(arr, &r)
		}
	}
	return arr, nil
}

func (s *MemoryStore) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	r := *runtime
	s.taskRuntimes[runtimeKey{runtime.TaskId, runtime.Id}] = &r
	return nil
}

func (s *MemoryStore) RemoveTaskRuntime(taskId, id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.taskRuntimes, runtimeKey{taskId, id})
	return nil
}

func (s *MemoryStore) ShouldTaskReloadItems(taskId, id string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if val, ok := s.taskReloadFlags[runtimeKey{taskId, id}]; ok {
		return val
	}
	return false
}

func (s *MemoryStore) RequireTaskReloadItems(taskId, id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.taskReloadFlags[runtimeKey{taskId, id}] = true
	return nil
}

func (s *MemoryStore) ClearTaskReloadItems(taskId, id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.taskReloadFlags[runtimeKey{taskId, id}] = false
	return nil
}

//
// task assignments
//

func (s *MemoryStore) GetTaskAssignment(taskId, itemId string) (*definition.TaskAssignment, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	t, ok := s.taskAssignments[runtimeKey{taskId, itemId}]
	if ok {
		r := *t
		return &r, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetTaskAssignments(taskId string) ([]*definition.TaskAssignment, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	arr := make([]*definition.TaskAssignment, 0, 1)
	for k, v := range s.taskAssignments {
		if k.left == taskId {
			r := *v
			arr = append(arr, &r)
		}
	}
	return arr, nil
}

func (s *MemoryStore) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	r := *assignment
	s.taskAssignments[runtimeKey{r.TaskId, r.ItemId}] = &r
	return nil
}

func (s *MemoryStore) RemoveTaskAssignment(taskId, itemId string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.taskAssignments, runtimeKey{taskId, itemId})
	return nil
}

//
// Strategy related
//

func (s *MemoryStore) GetStrategy(id string) (*definition.Strategy, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	strategy, ok := s.strategies[id]
	if ok {
		copyStrategy := *strategy
		return &copyStrategy, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetStrategies() ([]*definition.Strategy, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	list := make([]*definition.Strategy, 0, len(s.strategies))
	for _, strategy := range s.strategies {
		copyStrategy := *strategy
		list = append(list, &copyStrategy)
	}
	return list, nil
}

func (s *MemoryStore) CreateStrategy(strategy *definition.Strategy) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.strategies[strategy.Id]; ok {
		return store.AlreadyExist
	}
	copyStrategy := *strategy
	s.strategies[strategy.Id] = &copyStrategy
	return nil
}

func (s *MemoryStore) UpdateStrategy(strategy *definition.Strategy) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.strategies[strategy.Id]; !ok {
		return store.NotExist
	}
	copyStrategy := *strategy
	s.strategies[strategy.Id] = &copyStrategy
	return nil
}

func (s *MemoryStore) RemoveStrategy(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.strategies[id]; !ok {
		return store.NotExist
	}
	delete(s.strategies, id)
	return nil
}

//
// StrategyRuntime related
// (bind machine & strategy, 1 to 1 according to the strategy)
//

func (s *MemoryStore) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	t, ok := s.runtimes[runtimeKey{strategyId, schedulerId}]
	if ok {
		r := *t
		return &r, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	arr := make([]*definition.StrategyRuntime, 0, 1)
	for k, v := range s.runtimes {
		if k.left == strategyId {
			r := *v
			arr = append(arr, &r)
		}
	}
	return arr, nil
}

func (s *MemoryStore) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	r := *runtime
	s.runtimes[runtimeKey{runtime.StrategyId, runtime.SchedulerId}] = &r
	return nil
}

func (s *MemoryStore) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.runtimes, runtimeKey{strategyId, schedulerId})
	return nil
}

//
// Scheduler(Machine) related
//

func (s *MemoryStore) RegisterScheduler(scheduler *definition.Scheduler) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	copyScheduler := *scheduler
	s.schedulers[scheduler.Id] = &copyScheduler
	return nil
}
func (s *MemoryStore) UnregisterScheduler(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.schedulers[id]; !ok {
		return store.NotExist
	}
	delete(s.schedulers, id)
	return nil
}

func (s *MemoryStore) GetScheduler(id string) (*definition.Scheduler, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	scheduler, ok := s.schedulers[id]
	if ok {
		copyScheduler := *scheduler
		return &copyScheduler, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetSchedulers() ([]*definition.Scheduler, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	list := make([]*definition.Scheduler, 0, len(s.schedulers))
	for _, t := range s.schedulers {
		copyScheduler := *t
		list = append(list, &copyScheduler)
	}
	utils.SortSchedulers(list)
	return list, nil
}

func dumpMap(b *strings.Builder, k string, v interface{}) {
	b.WriteString(k)
	b.WriteString(": ")
	data, _ := json.Marshal(v)
	b.Write(data)
	b.WriteString("\n")
}

func (s *MemoryStore) Dump() string {
	b := &strings.Builder{}

	b.WriteString("Sequence:\n")
	b.WriteString(strconv.Itoa(int(s.sequence)))
	b.WriteString("\n")

	b.WriteString("\nTasks:\n")
	for k, v := range s.tasks {
		dumpMap(b, k, v)
	}

	b.WriteString("\nTaskRuntimes:\n")
	for k, v := range s.taskRuntimes {
		dumpMap(b, k.String(), v)
	}

	b.WriteString("\nTaskReloads:\n")
	for k, v := range s.taskReloadFlags {
		dumpMap(b, k.String(), v)
	}

	b.WriteString("\nTaskAssignments:\n")
	for k, v := range s.taskAssignments {
		dumpMap(b, k.String(), v)
	}

	b.WriteString("\nStrategies:\n")
	for k, v := range s.strategies {
		dumpMap(b, k, v)
	}

	b.WriteString("\nRuntimes:\n")
	for k, v := range s.runtimes {
		dumpMap(b, k.String(), v)
	}

	b.WriteString("\nSchedulers:\n")
	for k, v := range s.schedulers {
		dumpMap(b, k, v)
	}

	return b.String()
}
