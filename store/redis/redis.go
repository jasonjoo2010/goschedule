// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package redis

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/jasonjoo2010/enhanced-utils/concurrent/distlock"
	lockstore "github.com/jasonjoo2010/enhanced-utils/concurrent/distlock/redis"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/utils"
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
	lock   distlock.DistLock
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

func hasError(err error) bool {
	return err != nil && !strings.Contains(err.Error(), "redis: nil")
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
	if hasError(err) {
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
	if hasError(err) {
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
	if hasError(err) {
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

func parseTaskRuntime(str string, err error) (*definition.TaskRuntime, error) {
	if hasError(err) {
		return nil, err
	}
	if str == "" {
		return nil, store.NotExist
	}
	var runtime definition.TaskRuntime
	err = json.Unmarshal([]byte(str), &runtime)
	if err != nil {
		return nil, err
	}
	return &runtime, nil
}

func parseTaskAssignment(str string, err error) (*definition.TaskAssignment, error) {
	if hasError(err) {
		return nil, err
	}
	if str == "" {
		return nil, store.NotExist
	}
	var runtime definition.TaskAssignment
	err = json.Unmarshal([]byte(str), &runtime)
	if err != nil {
		return nil, err
	}
	return &runtime, nil
}

func parseScheduler(str string, err error) (*definition.Scheduler, error) {
	if hasError(err) {
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

func (s *RedisStore) keyTasks() string {
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

func (s *RedisStore) keyTaskRuntimes(strategyId, taskId string) string {
	return s.key("taskRuntimes/" + strategyId + "/" + taskId)
}

func (s *RedisStore) keyTaskItemsConfigVersion() string {
	return s.key("taskItemConfigVersion")
}

func (s *RedisStore) keyTaskAssignments(strategyId, taskId string) string {
	return s.key("taskAssignments/" + strategyId + "/" + taskId)
}

func (s *RedisStore) keySequence() string {
	return s.key("sequence")
}

func (s *RedisStore) Lock() distlock.DistLock {
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
	key := s.keySequence()
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
	key := s.keyTasks()
	return parseTask(s.client.HGet(key, id).Result())
}

func (s *RedisStore) GetTasks() ([]*definition.Task, error) {
	key := s.keyTasks()
	cnt := s.client.HLen(key).Val()
	if cnt == 0 {
		return []*definition.Task{}, nil
	}
	list := make([]*definition.Task, 0, cnt)
	keys := make(sort.StringSlice, 0, cnt)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	for k := range valMap {
		keys = append(keys, k)
	}
	keys.Sort()
	for _, k := range keys {
		task, err := parseTask(valMap[k], err)
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

	key := s.keyTasks()
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
	_, err = s.client.HSet(s.keyTasks(), task.Id, string(data)).Result()
	return err
}

func (s *RedisStore) RemoveTask(id string) error {
	cnt, err := s.client.HDel(s.keyTasks(), id).Result()
	if cnt == 0 {
		return store.NotExist
	}
	return err
}

//
// TaskRuntime related
// Task instance runtime
//

func (s *RedisStore) GetTaskRuntime(strategyId, taskId, id string) (*definition.TaskRuntime, error) {
	key := s.keyTaskRuntimes(strategyId, taskId)
	return parseTaskRuntime(s.client.HGet(key, id).Result())
}

func (s *RedisStore) GetTaskRuntimes(strategyId, taskId string) ([]*definition.TaskRuntime, error) {
	key := s.keyTaskRuntimes(strategyId, taskId)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	list := make([]*definition.TaskRuntime, 0, len(valMap))
	for _, v := range valMap {
		runtime, err := parseTaskRuntime(v, err)
		if err != nil {
			// ignore
			continue
		}
		list = append(list, runtime)
	}
	utils.SortTaskRuntimes(list)
	return list, nil
}

func (s *RedisStore) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	key := s.keyTaskRuntimes(runtime.StrategyId, runtime.TaskId)
	data, err := json.Marshal(runtime)
	if err != nil {
		return err
	}
	_, err = s.client.HSet(key, runtime.Id, string(data)).Result()
	return err
}

func (s *RedisStore) RemoveTaskRuntime(strategyId, taskId, id string) error {
	key := s.keyTaskRuntimes(strategyId, taskId)
	_, err := s.client.HDel(key, id).Result()
	return err
}

func (s *RedisStore) GetTaskItemsConfigVersion(strategyId, taskId string) (int64, error) {
	key := s.keyTaskItemsConfigVersion()
	subKey := strategyId + "/" + taskId
	val, err := s.client.HGet(key, subKey).Int64()
	if hasError(err) {
		return 0, err
	}
	return val, nil
}

func (s *RedisStore) IncreaseTaskItemsConfigVersion(strategyId, taskId string) error {
	key := s.keyTaskItemsConfigVersion()
	subKey := strategyId + "/" + taskId
	return s.client.HIncrBy(key, subKey, 1).Err()
}

//
// TaskAssignment related
//

func (s *RedisStore) GetTaskAssignment(strategyId, taskId, itemId string) (*definition.TaskAssignment, error) {
	key := s.keyTaskAssignments(strategyId, taskId)
	return parseTaskAssignment(s.client.HGet(key, itemId).Result())
}

func (s *RedisStore) GetTaskAssignments(strategyId, taskId string) ([]*definition.TaskAssignment, error) {
	key := s.keyTaskAssignments(strategyId, taskId)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	list := make([]*definition.TaskAssignment, 0, len(valMap))
	for _, v := range valMap {
		assignment, err := parseTaskAssignment(v, err)
		if err != nil {
			// ignore
			continue
		}
		list = append(list, assignment)
	}
	utils.SortTaskAssignments(list)
	return list, nil
}

func (s *RedisStore) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	key := s.keyTaskAssignments(assignment.StrategyId, assignment.TaskId)
	data, err := json.Marshal(assignment)
	if err != nil {
		return err
	}
	_, err = s.client.HSet(key, assignment.ItemId, string(data)).Result()
	return err
}

func (s *RedisStore) RemoveTaskAssignment(strategyId, taskId, itemId string) error {
	key := s.keyTaskAssignments(strategyId, taskId)
	_, err := s.client.HDel(key, itemId).Result()
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
	keys := make(sort.StringSlice, 0, cnt)
	valMap, err := s.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	for k := range valMap {
		keys = append(keys, k)
	}
	keys.Sort()
	for _, k := range keys {
		task, err := parseStrategy(valMap[k], err)
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

func (s *RedisStore) RemoveStrategy(id string) error {
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
	utils.SortStrategyRuntimes(list)
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
	return nil
}
func (s *RedisStore) UnregisterScheduler(id string) error {
	key := s.keySchedulers()
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
	size, err := s.client.HLen(key).Result()
	if err != nil {
		return make([]*definition.Scheduler, 0), err
	}
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
	utils.SortSchedulers(list)
	return list, nil
}

func dumpMap(b *strings.Builder, m map[string]string) {
	keys := make(sort.StringSlice, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	keys.Sort()
	for _, k := range keys {
		b.WriteString("\t")
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(m[k])
		b.WriteString("\n")
	}
}

func (s *RedisStore) Dump() string {
	b := &strings.Builder{}

	b.WriteString("Sequence:\n")
	b.WriteString(s.keySequence())
	b.WriteString(": ")
	b.WriteString(s.client.Get(s.keySequence()).Val())
	b.WriteString("\n")

	b.WriteString("\nTasks:\n")
	b.WriteString(s.keyTasks())
	b.WriteString(": \n")
	dumpMap(b, s.client.HGetAll(s.keyTasks()).Val())

	b.WriteString("\nTaskRuntimes:\n")
	tasks, _ := s.GetTasks()
	strategies, _ := s.GetStrategies()
	taskMap := make(map[string]*definition.Task, len(tasks))
	for _, task := range tasks {
		taskMap[task.Id] = task
	}
	for _, strategy := range strategies {
		if strategy.Kind != definition.TaskKind {
			continue
		}
		task := taskMap[strategy.Bind]
		if task == nil {
			continue
		}
		b.WriteString(s.keyTaskRuntimes(strategy.Id, task.Id))
		b.WriteString(":\n")
		dumpMap(b, s.client.HGetAll(s.keyTaskRuntimes(strategy.Id, task.Id)).Val())
	}

	b.WriteString("\nTaskItemsConfigVersion:\n")
	b.WriteString(s.keyTaskItemsConfigVersion())
	b.WriteString(": \n")
	dumpMap(b, s.client.HGetAll(s.keyTaskItemsConfigVersion()).Val())

	b.WriteString("\nTaskAssignments:\n")
	for _, strategy := range strategies {
		if strategy.Kind != definition.TaskKind {
			continue
		}
		task := taskMap[strategy.Bind]
		if task == nil {
			continue
		}
		b.WriteString(s.keyTaskAssignments(strategy.Id, task.Id))
		b.WriteString(":\n")
		dumpMap(b, s.client.HGetAll(s.keyTaskAssignments(strategy.Id, task.Id)).Val())
	}

	b.WriteString("\nStrategies:\n")
	b.WriteString(s.keyStrategies())
	b.WriteString(": \n")
	dumpMap(b, s.client.HGetAll(s.keyStrategies()).Val())

	b.WriteString("\nRuntimes:\n")
	for _, strategy := range strategies {
		b.WriteString(s.keyRuntimes(strategy.Id))
		b.WriteString(":\n")
		dumpMap(b, s.client.HGetAll(s.keyRuntimes(strategy.Id)).Val())
	}

	b.WriteString("\nSchedulers:\n")
	b.WriteString(s.keySchedulers())
	b.WriteString(": \n")
	dumpMap(b, s.client.HGetAll(s.keySchedulers()).Val())

	return b.String()
}
