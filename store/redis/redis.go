package redis

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/jasonjoo2010/enhanced-utils/concurrent/distlock"
	lockstore "github.com/jasonjoo2010/enhanced-utils/concurrent/distlock/redis"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
)

/**
KEYS GENERATED
- prefixed by {prefix}

sequence [autoincrement uint64]
tasks [map[string]json]
strategies [map[string]json]
schedulers [map[string]json]
**/

type RedisStoreConfig struct {
	// nodes/cluster nodes addresses
	Addrs []string
	// prefix which can isolate different applciations in same redis instance/cluster
	Prefix string
}

type RedisStore struct {
	client redis.UniversalClient
	prefix string
	lock   *distlock.DistLock
}

func NewFromConfig(config *RedisStoreConfig) *RedisStore {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: config.Addrs,

		PoolSize:           10,
		WriteTimeout:       time.Second * 5,
		ReadTimeout:        time.Second * 5,
		DialTimeout:        time.Second * 10,
		IdleTimeout:        time.Second * 180,
		PoolTimeout:        time.Second * 300,
		IdleCheckFrequency: time.Second * 60,
	})
	return &RedisStore{
		client: client,
		prefix: config.Prefix,
		lock:   distlock.NewMutex(config.Prefix, 60*time.Second, lockstore.New(config.Addrs)),
	}
}

func New(prefix, host string, port int) *RedisStore {
	return NewFromConfig(&RedisStoreConfig{
		Addrs: []string{
			host + ":" + strconv.Itoa(port),
		},
		Prefix: prefix,
	})
}

func parseTask(str string, err error) (*definition.Task, error) {
	if err != nil && !strings.Contains(err.Error(), "redis: nil") {
		return nil, err
	}
	if str == "" {
		return nil, store.NotExist
	}
	var task definition.Task
	err = json.Unmarshal([]byte(str), &task)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func parseStrategy(str string, err error) (*definition.Strategy, error) {
	if err != nil && !strings.Contains(err.Error(), "redis: nil") {
		return nil, err
	}
	if str == "" {
		return nil, store.NotExist
	}
	var strategy definition.Strategy
	err = json.Unmarshal([]byte(str), &strategy)
	if err != nil {
		return nil, err
	}
	return &strategy, nil
}

func parseRuntime(str string, err error) (*definition.StrategyRuntime, error) {
	if err != nil && !strings.Contains(err.Error(), "redis: nil") {
		return nil, err
	}
	if str == "" {
		return nil, store.NotExist
	}
	var runtime definition.StrategyRuntime
	err = json.Unmarshal([]byte(str), &runtime)
	if err != nil {
		return nil, err
	}
	return &runtime, nil
}

func parseScheduler(str string, err error) (*definition.Scheduler, error) {
	if err != nil && !strings.Contains(err.Error(), "redis: nil") {
		return nil, err
	}
	if str == "" {
		return nil, store.NotExist
	}
	var scheduler definition.Scheduler
	err = json.Unmarshal([]byte(str), &scheduler)
	if err != nil {
		return nil, err
	}
	return &scheduler, nil
}

func (s *RedisStore) key(k string) string {
	if s.prefix != "" {
		return s.prefix + ":" + k
	}
	return "scheduler:" + k
}

func (s *RedisStore) keyTask() string {
	return s.key("tasks")
}

func (s *RedisStore) keyStrategies() string {
	return s.key("strategies")
}

func (s *RedisStore) keySchedulers() string {
	return s.key("schedulers")
}

func (s *RedisStore) keyRuntimes(strategyId string) string {
	return s.key("runtimes/" + strategyId)
}

func (s *RedisStore) Lock() *distlock.DistLock {
	return s.lock
}

func (s *RedisStore) Name() string {
	return "redis"
}

func (s *RedisStore) Time() int64 {
	tm, err := s.client.Time().Result()
	if err != nil {
		return time.Now().UnixNano() / int64(time.Millisecond)
	}
	return tm.UnixNano() / int64(time.Millisecond)
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func (s *RedisStore) Sequence() (uint64, error) {
	key := s.key("sequence")
	for i := 0; i < 2; i++ {
		val, err := s.client.Incr(key).Result()
		if err != nil {
			return 0, err
		}
		if val < 1 {
			s.client.Del(key)
		}
		return uint64(val), nil
	}
	return 0, errors.New("Can not get global sequence from redis store")
}

//
// Task related
//

func (s *RedisStore) GetTask(id string) (*definition.Task, error) {
	key := s.keyTask()
	return parseTask(s.client.HGet(key, id).Result())
}

func (s *RedisStore) GetTasks() ([]*definition.Task, error) {
	key := s.keyTask()
	cnt := s.client.HLen(key).Val()
	if cnt == 0 {
		return []*definition.Task{}, nil
	}
	list := make([]*definition.Task, 0, cnt)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	for _, v := range valMap {
		task, err := parseTask(v, err)
		if err != nil {
			// ignore
			continue
		}
		list = append(list, task)
	}
	return list, nil
}

func (s *RedisStore) CreateTask(task *definition.Task) error {
	if _, err := s.GetTask(task.Id); err == nil {
		return store.AlreadyExist
	}

	key := s.keyTask()
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = s.client.HSetNX(key, task.Id, string(data)).Result()
	return err
}

func (s *RedisStore) UpdateTask(task *definition.Task) error {
	if _, err := s.GetTask(task.Id); err != nil {
		return store.NotExist
	}
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = s.client.HSet(s.keyTask(), task.Id, string(data)).Result()
	return err
}

func (s *RedisStore) DeleteTask(id string) error {
	cnt, err := s.client.HDel(s.keyTask(), id).Result()
	if cnt == 0 {
		return store.NotExist
	}
	return err
}

//
// Strategy related
//

func (s *RedisStore) GetStrategy(id string) (*definition.Strategy, error) {
	key := s.keyStrategies()
	return parseStrategy(s.client.HGet(key, id).Result())
}

func (s *RedisStore) GetStrategies() ([]*definition.Strategy, error) {
	key := s.keyStrategies()
	cnt := s.client.HLen(key).Val()
	if cnt == 0 {
		return []*definition.Strategy{}, nil
	}
	list := make([]*definition.Strategy, 0, cnt)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	for _, v := range valMap {
		task, err := parseStrategy(v, err)
		if err != nil {
			// ignore
			continue
		}
		list = append(list, task)
	}
	return list, nil
}

func (s *RedisStore) CreateStrategy(strategy *definition.Strategy) error {
	if _, err := s.GetStrategy(strategy.Id); err == nil {
		return store.AlreadyExist
	}

	key := s.keyStrategies()
	data, err := json.Marshal(strategy)
	if err != nil {
		return err
	}
	_, err = s.client.HSetNX(key, strategy.Id, string(data)).Result()
	return err
}

func (s *RedisStore) UpdateStrategy(strategy *definition.Strategy) error {
	if _, err := s.GetStrategy(strategy.Id); err != nil {
		return store.NotExist
	}
	data, err := json.Marshal(strategy)
	if err != nil {
		return err
	}
	_, err = s.client.HSet(s.keyStrategies(), strategy.Id, string(data)).Result()
	return err
}

func (s *RedisStore) DeleteStrategy(id string) error {
	cnt, err := s.client.HDel(s.keyStrategies(), id).Result()
	if cnt == 0 {
		return store.NotExist
	}
	return err
}

//
// StrategyRuntime related
// (bind machine & strategy, 1 to 1 according to the strategy)
//

func (s *RedisStore) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	key := s.keyRuntimes(strategyId)
	return parseRuntime(s.client.HGet(key, schedulerId).Result())
}

func (s *RedisStore) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
	key := s.keyRuntimes(strategyId)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	list := make([]*definition.StrategyRuntime, 0, len(valMap))
	for _, v := range valMap {
		runtime, err := parseRuntime(v, err)
		if err != nil {
			// ignore
			continue
		}
		list = append(list, runtime)
	}
	return list, nil
}

func (s *RedisStore) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	key := s.keyRuntimes(runtime.StrategyId)
	data, err := json.Marshal(runtime)
	if err != nil {
		return err
	}
	_, err = s.client.HSet(key, runtime.SchedulerId, string(data)).Result()
	return err
}

func (s *RedisStore) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	key := s.keyRuntimes(strategyId)
	_, err := s.client.HDel(key, schedulerId).Result()
	return err
}

//
// Scheduler(Machine) related
//

func (s *RedisStore) RegisterScheduler(scheduler *definition.Scheduler) error {
	key := s.keySchedulers()
	data, err := json.Marshal(scheduler)
	if err != nil {
		// Now just ignore it
		return errors.New("Serialize scheduler object failed")
	}
	s.client.HSet(key, scheduler.Id, string(data)).Result()
	// XXX: Runtime operations registering migrated to upper logic
	return nil
}
func (s *RedisStore) UnregisterScheduler(id string) error {
	key := s.keySchedulers()
	// XXX: remove strategy runtime binded to this scheduler
	s.client.HDel(key, id)
	return nil
}

func (s *RedisStore) GetScheduler(id string) (*definition.Scheduler, error) {
	key := s.keySchedulers()
	return parseScheduler(s.client.HGet(key, id).Result())
}

func (s *RedisStore) GetSchedulers() ([]*definition.Scheduler, error) {
	cur := uint64(0)
	key := s.keySchedulers()
	size := s.client.HLen(key).Val()
	page := int64(20)
	if size < 1 {
		return []*definition.Scheduler{}, nil
	}
	list := make([]*definition.Scheduler, 0, size)
	keys_visited := make(map[string]bool)
	// max loop count
	max_loops := int(math.Max(float64(size/page), 1) * 5)
	for i := 0; i < max_loops; i++ {
		keys, c, err := s.client.HScan(key, cur, "*", page).Result()
		if err != nil || len(keys) == 0 {
			break
		}
		for index, item := range keys {
			if index%2 == 0 {
				// skip keys
				continue
			}
			obj, err := parseScheduler(item, nil)
			if err != nil || keys_visited[obj.Id] {
				continue
			}
			keys_visited[obj.Id] = true
			list = append(list, obj)
		}
		if c == 0 {
			break
		}
		cur = c
	}
	return list, nil
}
