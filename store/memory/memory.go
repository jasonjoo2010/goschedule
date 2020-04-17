package memory

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
)

type MemoryStore struct {
	sequence   uint64
	lock       *sync.Mutex // because is in-memory store so only one lock is shared currently is enough i think
	tasks      map[string]*definition.Task
	strategies map[string]*definition.Strategy
	schedulers map[string]*definition.Scheduler
	runtimes   map[runtimeKey]*definition.StrategyRuntime
}

type runtimeKey struct {
	StrategyId, SchedulerId string
}

func New() *MemoryStore {
	return &MemoryStore{
		lock:       &sync.Mutex{},
		tasks:      make(map[string]*definition.Task),
		strategies: make(map[string]*definition.Strategy),
		schedulers: make(map[string]*definition.Scheduler),
		runtimes:   make(map[runtimeKey]*definition.StrategyRuntime),
	}
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
	s.lock.Lock()
	defer s.lock.Unlock()
	task, ok := s.tasks[id]
	if ok {
		return task, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetTasks() ([]*definition.Task, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	list := make([]*definition.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		list = append(list, t)
	}
	return list, nil
}

func (s *MemoryStore) CreateTask(task *definition.Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.tasks[task.Id]; ok {
		return store.AlreadyExist
	}
	s.tasks[task.Id] = task
	return nil
}

func (s *MemoryStore) UpdateTask(task *definition.Task) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.tasks[task.Id]; !ok {
		return store.NotExist
	}
	s.tasks[task.Id] = task
	return nil
}

func (s *MemoryStore) DeleteTask(id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return store.NotExist
	}
	delete(s.tasks, id)
	return nil
}

//
// Strategy related
//

func (s *MemoryStore) GetStrategy(id string) (*definition.Strategy, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	strategy, ok := s.strategies[id]
	if ok {
		return strategy, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetStrategies() ([]*definition.Strategy, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	list := make([]*definition.Strategy, 0, len(s.strategies))
	for _, t := range s.strategies {
		list = append(list, t)
	}
	return list, nil
}

func (s *MemoryStore) CreateStrategy(strategy *definition.Strategy) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.strategies[strategy.Id]; ok {
		return store.AlreadyExist
	}
	s.strategies[strategy.Id] = strategy
	return nil
}

func (s *MemoryStore) UpdateStrategy(strategy *definition.Strategy) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.strategies[strategy.Id]; !ok {
		return store.NotExist
	}
	s.strategies[strategy.Id] = strategy
	return nil
}

func (s *MemoryStore) DeleteStrategy(id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
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
	s.lock.Lock()
	defer s.lock.Unlock()
	t, ok := s.runtimes[runtimeKey{strategyId, schedulerId}]
	if ok {
		return t, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	arr := make([]*definition.StrategyRuntime, 1)
	for k, v := range s.runtimes {
		if k.StrategyId == strategyId {
			arr = append(arr, v)
		}
	}
	return arr, nil
}

func (s *MemoryStore) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.runtimes[runtimeKey{runtime.StrategyId, runtime.SchedulerId}] = runtime
	return nil
}

func (s *MemoryStore) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.runtimes, runtimeKey{strategyId, schedulerId})
	return nil
}

//
// Scheduler(Machine) related
//

func (s *MemoryStore) RegisterScheduler(scheduler *definition.Scheduler) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.schedulers[scheduler.Id] = scheduler
}
func (s *MemoryStore) UnregisterScheduler(id string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.schedulers, id)
}

func (s *MemoryStore) GetScheduler(id string) (*definition.Scheduler, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	scheduler, ok := s.schedulers[id]
	if ok {
		return scheduler, nil
	}
	return nil, store.NotExist
}

func (s *MemoryStore) GetSchedulers() []*definition.Scheduler {
	s.lock.Lock()
	defer s.lock.Unlock()
	list := make([]*definition.Scheduler, 0, len(s.schedulers))
	for _, t := range s.schedulers {
		list = append(list, t)
	}
	return list
}
