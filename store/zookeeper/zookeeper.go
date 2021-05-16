// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

type getElementFunction func(string) (interface{}, error)

type ZookeeperStoreConfig struct {
	// nodes/cluster nodes addresses
	Addrs []string
	// prefix which can isolate different applciations in same instance/cluster
	BasePath string
	Username string
	Password string
}

type ZookeeperStore struct {
	prefix    string
	conn      *zk.Conn
	acl       []zk.ACL
	timeDelta time.Duration
}

func New(basePath, addr string, port int) *ZookeeperStore {
	return NewFromConfig(&ZookeeperStoreConfig{
		Addrs:    []string{addr + ":" + strconv.Itoa(port)},
		BasePath: basePath,
	})
}

func NewFromConfig(config *ZookeeperStoreConfig) *ZookeeperStore {
	s := &ZookeeperStore{
		prefix: strings.TrimRight(config.BasePath, "/"),
	}

	conn, eventC, err := zk.Connect(config.Addrs, 60*time.Second,
		zk.WithLogger(logrus.StandardLogger()),
		zk.WithEventCallback(s.onEvent),
		zk.WithLogInfo(true),
	)
	if err != nil {
		return nil
	}
	if config.Username != "" {
		auth := zk.DigestACL(zk.PermAll, config.Username, config.Password)[0]
		conn.AddAuth(auth.Scheme, []byte(config.Username+":"+config.Password))
		s.acl = append(s.acl, auth, zk.WorldACL(zk.PermRead)[0])
	}
	s.conn = conn
	// wait for connected
	timeout := time.NewTimer(10 * time.Second)
CHECK_LOOP:
	for {
		select {
		case event := <-eventC:
			if event.State == zk.StateHasSession {
				logrus.Info("Connected to zookeeper server: ", event.Server)
				break CHECK_LOOP
			}
		case <-timeout.C:
			conn.Close()
			logrus.Error("Failed to connect to zookeeper server: timeout")
			return nil
		}
	}
	timeout.Stop()
	s.verifyPrefix()
	s.determineTimeDelta()
	return s
}

func (s *ZookeeperStore) key(path string) string {
	return s.prefix + path
}

func (s *ZookeeperStore) Name() string {
	return "zookeeper"
}

func (s *ZookeeperStore) Time() int64 {
	return time.Now().Add(s.timeDelta).UnixNano() / 1e6
}

func (s *ZookeeperStore) Sequence() (uint64, error) {
	basePath := s.key("/seq:")
	path, err := s.conn.Create(basePath, nil, zk.FlagEphemeral|zk.FlagSequence, s.acl)
	if err != nil {
		return 0, err
	}
	s.conn.Delete(path, 0)
	sequence, err := strconv.ParseInt(path[len(basePath):], 10, 64)
	if err != nil {
		return 0, err
	}
	return uint64(sequence), nil
}

func (s *ZookeeperStore) Close() error {
	s.conn.Close()
	s.conn = nil
	return nil
}

func (s *ZookeeperStore) keyScheduler(id string) string {
	return s.keySchedulers() + "/" + id
}

func (s *ZookeeperStore) keySchedulers() string {
	return s.key("/schedulers")
}

func (s *ZookeeperStore) keyTask(id string) string {
	return s.keyTasks() + "/" + id
}

func (s *ZookeeperStore) keyTasks() string {
	return s.key("/tasks")
}

func (s *ZookeeperStore) keyStrategy(id string) string {
	return s.keyStrategies() + "/" + id
}

func (s *ZookeeperStore) keyStrategies() string {
	return s.key("/strategies")
}

func (s *ZookeeperStore) keyStrategyRuntime(strategyId, schedulerId string) string {
	return s.keyStrategyRuntimes(strategyId) + "/" + schedulerId
}

func (s *ZookeeperStore) keyStrategyRuntimes(strategyId string) string {
	return s.keyStrategyRuntimesBase() + "/" + strategyId
}

func (s *ZookeeperStore) keyStrategyRuntimesBase() string {
	return s.key("/runtimes")
}

func (s *ZookeeperStore) keyTaskRuntime(strategyId, taskId, runtimeId string) string {
	return s.keyTaskRuntimes(strategyId, taskId) + "/" + runtimeId
}

func (s *ZookeeperStore) keyTaskAssignment(strategyId, taskId, itemId string) string {
	return s.keyTaskAssignments(strategyId, taskId) + "/" + itemId
}

func (s *ZookeeperStore) keyTaskRuntimes(strategyId, taskId string) string {
	return s.keyTaskInfo(strategyId, taskId) + "/runtimes"
}

func (s *ZookeeperStore) keyTaskAssignments(strategyId, taskId string) string {
	return s.keyTaskInfo(strategyId, taskId) + "/assignments"
}

func (s *ZookeeperStore) keyTaskInfo(strategyId, taskId string) string {
	return s.keyTaskInfoBase() + "/" + strategyId + "/" + taskId
}

func (s *ZookeeperStore) keyTaskInfoBase() string {
	return s.key("/runningInfo")
}

func (s *ZookeeperStore) setTemporaryNode(key string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		logrus.Warn("encode obj failed: ", err.Error())
		return err
	}
	if s.exists(key) {
		_, err = s.conn.Set(key, data, -1)
		return err
	}
	_, err = s.conn.Create(key, data, zk.FlagEphemeral, s.acl)
	return err
}

func (s *ZookeeperStore) getItems(base_path string, getFunc getElementFunction) ([]interface{}, error) {
	ids, _, err := s.conn.Children(base_path)
	if err != nil {
		return nil, err
	}
	if len(ids) < 1 {
		return []interface{}{}, nil
	}
	result := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		obj, err := getFunc(id)
		if err != nil {
			logrus.Warn("Fetch object failed: ", err.Error())
			continue
		}
		if obj == nil {
			// should not happen
			continue
		}
		result = append(result, obj)
	}
	return result, nil
}

// scheduler related

func (s *ZookeeperStore) RegisterScheduler(scheduler *definition.Scheduler) error {
	return s.setTemporaryNode(s.keyScheduler(scheduler.Id), scheduler)
}

func (s *ZookeeperStore) UnregisterScheduler(id string) error {
	s.conn.Delete(s.keyScheduler(id), -1)
	return nil
}

func (s *ZookeeperStore) GetSchedulers() ([]*definition.Scheduler, error) {
	arr, err := s.getItems(s.keySchedulers(), func(id string) (interface{}, error) {
		return s.GetScheduler(id)
	})
	if err != nil {
		return nil, err
	}
	result := make([]*definition.Scheduler, len(arr))
	for i := range arr {
		result[i] = arr[i].(*definition.Scheduler)
	}
	return result, nil
}

func (s *ZookeeperStore) GetScheduler(id string) (*definition.Scheduler, error) {
	key := s.keyScheduler(id)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return nil, store.NotExist
	}
	if err != nil {
		return nil, err
	}
	scheduler := &definition.Scheduler{}
	err = json.Unmarshal(data, scheduler)
	if err != nil {
		return nil, err
	}
	return scheduler, nil
}

// task related

func (s *ZookeeperStore) GetTask(id string) (*definition.Task, error) {
	key := s.keyTask(id)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return nil, store.NotExist
	}
	if err != nil {
		return nil, err
	}
	task := &definition.Task{}
	err = json.Unmarshal(data, task)
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (s *ZookeeperStore) GetTasks() ([]*definition.Task, error) {
	arr, err := s.getItems(s.keyTasks(), func(id string) (interface{}, error) {
		return s.GetTask(id)
	})
	if err != nil {
		return nil, err
	}
	result := make([]*definition.Task, len(arr))
	for i := range arr {
		result[i] = arr[i].(*definition.Task)
	}
	return result, nil
}

func (s *ZookeeperStore) CreateTask(task *definition.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = s.conn.Create(s.keyTask(task.Id), data, 0, s.acl)
	if err == zk.ErrNodeExists {
		return store.AlreadyExist
	}
	return err
}

func (s *ZookeeperStore) UpdateTask(task *definition.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = s.conn.Set(s.keyTask(task.Id), data, -1)
	if err == zk.ErrNoNode {
		return store.NotExist
	}
	return err
}

func (s *ZookeeperStore) RemoveTask(id string) error {
	err := s.conn.Delete(s.keyTask(id), -1)
	if err == zk.ErrNoNode {
		return store.NotExist
	}
	return err
}

// task runtime related

func (s *ZookeeperStore) GetTaskRuntime(strategyId, taskId, id string) (*definition.TaskRuntime, error) {
	key := s.keyTaskRuntime(strategyId, taskId, id)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return nil, store.NotExist
	}
	if err != nil {
		return nil, err
	}
	runtime := &definition.TaskRuntime{}
	err = json.Unmarshal(data, runtime)
	if err != nil {
		return nil, err
	}
	return runtime, nil
}
func (s *ZookeeperStore) GetTaskRuntimes(strategyId, taskId string) ([]*definition.TaskRuntime, error) {
	arr, err := s.getItems(s.keyTaskRuntimes(strategyId, taskId), func(id string) (interface{}, error) {
		return s.GetTaskRuntime(strategyId, taskId, id)
	})
	if err == zk.ErrNoNode {
		return []*definition.TaskRuntime{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]*definition.TaskRuntime, len(arr))
	for i := range arr {
		result[i] = arr[i].(*definition.TaskRuntime)
	}
	return result, nil
}
func (s *ZookeeperStore) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	data, err := json.Marshal(runtime)
	if err != nil {
		return err
	}
	key := s.keyTaskRuntime(runtime.StrategyId, runtime.TaskId, runtime.Id)
	if s.exists(key) {
		_, err = s.conn.Set(key, data, -1)
	} else {
		_, err = s.conn.Create(key, data, 0, s.acl)
		if err == zk.ErrNoNode {
			// make sure parent existed and recreate
			baseKey := s.keyTaskRuntimes(runtime.StrategyId, runtime.TaskId)
			if !s.exists(baseKey) {
				s.createPath(baseKey, true)
			}
			_, err = s.conn.Create(key, data, 0, s.acl)
		}
	}
	if err == zk.ErrNoNode || err == zk.ErrNodeExists {
		return nil
	}
	return err
}
func (s *ZookeeperStore) RemoveTaskRuntime(strategyId, taskId, id string) error {
	err := s.conn.Delete(s.keyTaskRuntime(strategyId, taskId, id), -1)
	if err == zk.ErrNoNode {
		return nil
	}
	return err
}

// task assignment related

func (s *ZookeeperStore) GetTaskItemsConfigVersion(strategyId, taskId string) (int64, error) {
	key := s.keyTaskAssignments(strategyId, taskId)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil
	}
	version := string(data)
	ver, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return 0, err
	}
	return ver, nil
}
func (s *ZookeeperStore) IncreaseTaskItemsConfigVersion(strategyId, taskId string) error {
	key := s.keyTaskAssignments(strategyId, taskId)
	for {
		data, stat, err := s.conn.Get(key)
		if err == zk.ErrNoNode {
			s.createPath(key, true)
			continue
		}
		if err != nil {
			return err
		}
		var ver int64
		if data != nil {
			n, err := strconv.ParseInt(string(data), 10, 64)
			if err == nil {
				ver = n
			}
		}
		val := strconv.FormatInt(ver+int64(1), 10)
		_, err = s.conn.Set(key, []byte(val), stat.Version)
		if err == zk.ErrBadVersion {
			// retry
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}
}

func (s *ZookeeperStore) GetTaskAssignment(strategyId, taskId, itemId string) (*definition.TaskAssignment, error) {
	key := s.keyTaskAssignment(strategyId, taskId, itemId)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return nil, store.NotExist
	}
	if err != nil {
		return nil, err
	}
	runtime := &definition.TaskAssignment{}
	err = json.Unmarshal(data, runtime)
	if err != nil {
		return nil, err
	}
	return runtime, nil
}
func (s *ZookeeperStore) GetTaskAssignments(strategyId, taskId string) ([]*definition.TaskAssignment, error) {
	arr, err := s.getItems(s.keyTaskAssignments(strategyId, taskId), func(id string) (interface{}, error) {
		return s.GetTaskAssignment(strategyId, taskId, id)
	})
	if err == zk.ErrNoNode {
		return []*definition.TaskAssignment{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]*definition.TaskAssignment, len(arr))
	for i := range arr {
		result[i] = arr[i].(*definition.TaskAssignment)
	}
	return result, nil
}
func (s *ZookeeperStore) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	data, err := json.Marshal(assignment)
	if err != nil {
		return err
	}
	key := s.keyTaskAssignment(assignment.StrategyId, assignment.TaskId, assignment.ItemId)
	if s.exists(key) {
		_, err = s.conn.Set(key, data, -1)
	} else {
		_, err = s.conn.Create(key, data, 0, s.acl)
		if err == zk.ErrNoNode {
			// make sure parent existed and recreate
			baseKey := s.keyTaskAssignments(assignment.StrategyId, assignment.TaskId)
			if !s.exists(baseKey) {
				s.createPath(baseKey, true)
			}
			_, err = s.conn.Create(key, data, 0, s.acl)
		}
	}
	if err == zk.ErrNoNode || err == zk.ErrNodeExists {
		return nil
	}
	return err
}
func (s *ZookeeperStore) RemoveTaskAssignment(strategyId, taskId, itemId string) error {
	err := s.conn.Delete(s.keyTaskAssignment(strategyId, taskId, itemId), -1)
	if err == zk.ErrNoNode {
		return nil
	}
	return err
}

// strategy related

func (s *ZookeeperStore) GetStrategy(id string) (*definition.Strategy, error) {
	key := s.keyStrategy(id)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return nil, store.NotExist
	}
	if err != nil {
		return nil, err
	}
	strategy := &definition.Strategy{}
	err = json.Unmarshal(data, strategy)
	if err != nil {
		return nil, err
	}
	return strategy, nil
}

func (s *ZookeeperStore) GetStrategies() ([]*definition.Strategy, error) {
	arr, err := s.getItems(s.keyStrategies(), func(id string) (interface{}, error) {
		return s.GetStrategy(id)
	})
	if err != nil {
		return nil, err
	}
	result := make([]*definition.Strategy, len(arr))
	for i := range arr {
		result[i] = arr[i].(*definition.Strategy)
	}
	return result, nil
}

func (s *ZookeeperStore) CreateStrategy(strategy *definition.Strategy) error {
	data, err := json.Marshal(strategy)
	if err != nil {
		return err
	}
	_, err = s.conn.Create(s.keyStrategy(strategy.Id), data, 0, s.acl)
	if err == zk.ErrNodeExists {
		return store.AlreadyExist
	}
	return err
}

func (s *ZookeeperStore) UpdateStrategy(strategy *definition.Strategy) error {
	data, err := json.Marshal(strategy)
	if err != nil {
		return err
	}
	_, err = s.conn.Set(s.keyStrategy(strategy.Id), data, -1)
	if err == zk.ErrNoNode {
		return store.NotExist
	}
	return err
}

func (s *ZookeeperStore) RemoveStrategy(id string) error {
	err := s.conn.Delete(s.keyStrategy(id), -1)
	if err == zk.ErrNoNode {
		return store.NotExist
	}
	return err
}

// strategy runtime related

func (s *ZookeeperStore) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	key := s.keyStrategyRuntime(strategyId, schedulerId)
	data, _, err := s.conn.Get(key)
	if err == zk.ErrNoNode {
		return nil, store.NotExist
	}
	if err != nil {
		return nil, err
	}
	runtime := &definition.StrategyRuntime{}
	err = json.Unmarshal(data, runtime)
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

func (s *ZookeeperStore) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
	arr, err := s.getItems(s.keyStrategyRuntimes(strategyId), func(id string) (interface{}, error) {
		return s.GetStrategyRuntime(strategyId, id)
	})
	if err == zk.ErrNoNode {
		return []*definition.StrategyRuntime{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]*definition.StrategyRuntime, len(arr))
	for i := range arr {
		result[i] = arr[i].(*definition.StrategyRuntime)
	}
	return result, nil
}

func (s *ZookeeperStore) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	data, err := json.Marshal(runtime)
	if err != nil {
		return err
	}
	key := s.keyStrategyRuntime(runtime.StrategyId, runtime.SchedulerId)
	if s.exists(key) {
		_, err = s.conn.Set(key, data, -1)
	} else {
		_, err = s.conn.Create(key, data, zk.FlagEphemeral, s.acl)
		if err == zk.ErrNoNode {
			// make sure parent existed and recreate
			baseKey := s.keyStrategyRuntimes(runtime.StrategyId)
			if !s.exists(baseKey) {
				s.createPath(baseKey, true)
			}
			_, err = s.conn.Create(key, data, zk.FlagEphemeral, s.acl)
		}
	}
	if err == zk.ErrNoNode || err == zk.ErrNodeExists {
		return nil
	}
	return err
}

func (s *ZookeeperStore) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	err := s.conn.Delete(s.keyStrategyRuntime(strategyId, schedulerId), -1)
	if err == zk.ErrNoNode {
		return nil
	}
	return err
}

func (s *ZookeeperStore) Dump() string {
	arr := s.getChildren(s.prefix, true)
	b := strings.Builder{}
	for _, path := range arr {
		b.WriteString(path)
		b.WriteString(": ")
		data, _, _ := s.conn.Get(path)
		b.Write(data)
		b.WriteString("\n")
	}
	return b.String()
}
