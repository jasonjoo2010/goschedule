// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package zookeeper

import (
	"strconv"
	"strings"
	"time"

	"github.com/jasonjoo2010/enhanced-utils/concurrent/distlock"
	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

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
	lock      *distlock.DistLock
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

func (s *ZookeeperStore) Name() string {
	return "zookeeper"
}

func (s *ZookeeperStore) Time() int64 {
	return time.Now().Add(s.timeDelta).UnixNano() / 1e6
}

func (s *ZookeeperStore) Sequence() (uint64, error) {
	return 0, nil
}

func (s *ZookeeperStore) Close() error {
	// TODO
	return nil
}

func (s *ZookeeperStore) Lock() *distlock.DistLock {
	// TODO
	return nil
}

func (s *ZookeeperStore) RegisterScheduler(scheduler *definition.Scheduler) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) UnregisterScheduler(id string) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) GetSchedulers() ([]*definition.Scheduler, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) GetScheduler(id string) (*definition.Scheduler, error) {
	// TODO
	return nil, nil
}

func (s *ZookeeperStore) GetTask(id string) (*definition.Task, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) GetTasks() ([]*definition.Task, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) CreateTask(task *definition.Task) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) UpdateTask(task *definition.Task) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) RemoveTask(id string) error {
	// TODO
	return nil
}

func (s *ZookeeperStore) GetTaskRuntime(strategyId, taskId, id string) (*definition.TaskRuntime, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) GetTaskRuntimes(strategyId, taskId string) ([]*definition.TaskRuntime, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) RemoveTaskRuntime(strategyId, taskId, id string) error {
	// TODO
	return nil
}

func (s *ZookeeperStore) GetTaskItemsConfigVersion(strategyId, taskId string) (int64, error) {
	// TODO
	return 0, nil
}
func (s *ZookeeperStore) IncreaseTaskItemsConfigVersion(strategyId, taskId string) error {
	// TODO
	return nil
}

func (s *ZookeeperStore) GetTaskAssignment(strategyId, taskId, itemId string) (*definition.TaskAssignment, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) GetTaskAssignments(strategyId, taskId string) ([]*definition.TaskAssignment, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) RemoveTaskAssignment(strategyId, taskId, itemId string) error {
	// TODO
	return nil
}

func (s *ZookeeperStore) GetStrategy(id string) (*definition.Strategy, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) GetStrategies() ([]*definition.Strategy, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) CreateStrategy(strategy *definition.Strategy) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) UpdateStrategy(strategy *definition.Strategy) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) RemoveStrategy(id string) error {
	// TODO
	return nil
}

func (s *ZookeeperStore) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
	// TODO
	return nil, nil
}
func (s *ZookeeperStore) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	// TODO
	return nil
}
func (s *ZookeeperStore) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	// TODO
	return nil
}

func (s *ZookeeperStore) Dump() string {
	// TODO
	return ""
}
