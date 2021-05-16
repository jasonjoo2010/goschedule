// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv3

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/sirupsen/logrus"
)

type Etcdv3Store struct {
	client   *etcd.Client
	kvApi    etcd.KV
	leaseApi etcd.Lease
	prefix   string
	stopped  bool
}

func (s *Etcdv3Store) keySequenceBase() string {
	return s.prefix + "/sequence"
}

func (s *Etcdv3Store) keyTasks() string {
	return s.prefix + "/tasks"
}

func (s *Etcdv3Store) keyTask(id string) string {
	return s.keyTasks() + "/" + id
}

func (s *Etcdv3Store) keyStrategies() string {
	return s.prefix + "/strategies"
}

func (s *Etcdv3Store) keyStrategy(id string) string {
	return s.keyStrategies() + "/" + id
}

func (s *Etcdv3Store) keySchedulers() string {
	return s.prefix + "/schedulers"
}

func (s *Etcdv3Store) keyScheduler(id string) string {
	return s.keySchedulers() + "/" + id
}

func (s *Etcdv3Store) keyRuntime(strategyId, schedulerId string) string {
	return s.keyRuntimes(strategyId) + "/" + schedulerId
}

func (s *Etcdv3Store) keyRuntimes(strategyId string) string {
	return s.prefix + "/runtimes/" + strategyId
}

func (s *Etcdv3Store) keyTaskRuntime(strategyId, taskId, runtimeId string) string {
	return s.keyTaskRuntimes(strategyId, taskId) + "/" + runtimeId
}

func (s *Etcdv3Store) keyTaskRuntimes(strategyId, taskId string) string {
	return s.prefix + "/taskRuntimes/" + strategyId + "/" + taskId
}

func (s *Etcdv3Store) keyTaskAssignment(strategyId, taskId, itemId string) string {
	return s.keyTaskAssignments(strategyId, taskId) + "/" + itemId
}

func (s *Etcdv3Store) keyTaskAssignments(strategyId, taskId string) string {
	return s.prefix + "/taskAssignments/" + strategyId + "/" + taskId
}

func (s *Etcdv3Store) keyTaskReload(strategyId, taskId string) string {
	return s.prefix + "/taskReload/" + strategyId + "/" + taskId
}

func (s *Etcdv3Store) getObject(key string, obj interface{}) error {
	resp, err := s.kvApi.Get(context.Background(), key)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return store.NotExist
	}
	str := string(resp.Kvs[0].Value)
	if str == "" {
		return store.NotExist
	}
	return json.Unmarshal([]byte(str), obj)
}

func (s *Etcdv3Store) getObjects(basepath string, t reflect.Type) ([]interface{}, error) {
	resp, err := s.kvApi.Get(context.Background(), basepath+"/",
		etcd.WithPrefix(),
		etcd.WithLimit(1000),
	)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, resp.Count)
	for _, n := range resp.Kvs {
		obj := reflect.New(t).Interface()
		err = json.Unmarshal(n.Value, obj)
		if err != nil {
			logrus.Warn("Wrong data type during deserializing: " + string(n.Key))
			continue
		}
		result = append(result, obj)
	}
	return result, nil
}

func (s *Etcdv3Store) Name() string {
	return "etcdv3"
}

func (s *Etcdv3Store) Time() int64 {
	return time.Now().UnixNano() / 1e6
}

func (s *Etcdv3Store) Sequence() (uint64, error) {
	resp, err := s.kvApi.Put(context.Background(), s.keySequenceBase(), "", etcd.WithPrevKV())
	if err != nil {
		return 0, err
	}
	if resp.PrevKv == nil {
		return 0, nil
	}
	return uint64(resp.PrevKv.Version), nil
}

func (s *Etcdv3Store) Close() error {
	if s.stopped {
		return nil
	}
	s.stopped = true
	s.client.Close()
	return nil
}

func (s *Etcdv3Store) RegisterScheduler(scheduler *definition.Scheduler) error {
	if scheduler == nil {
		return errors.New("scheduler should not be nil")
	}
	return s.update(s.keyScheduler(scheduler.ID), scheduler, false)
}

func (s *Etcdv3Store) UnregisterScheduler(id string) error {
	err := s.remove(s.keyScheduler(id), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv3Store) GetSchedulers() ([]*definition.Scheduler, error) {
	arr, err := s.getObjects(s.keySchedulers(), reflect.TypeOf(definition.Scheduler{}))
	if err == store.NotExist {
		return []*definition.Scheduler{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]*definition.Scheduler, 0, len(arr))
	for _, obj := range arr {
		s, ok := obj.(*definition.Scheduler)
		if !ok {
			continue
		}
		result = append(result, s)
	}
	return result, nil
}

func (s *Etcdv3Store) GetScheduler(id string) (*definition.Scheduler, error) {
	obj := &definition.Scheduler{}
	err := s.getObject(s.keyScheduler(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv3Store) GetTask(id string) (*definition.Task, error) {
	obj := &definition.Task{}
	err := s.getObject(s.keyTask(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv3Store) GetTasks() ([]*definition.Task, error) {
	arr, err := s.getObjects(s.keyTasks(), reflect.TypeOf(definition.Task{}))
	if err != nil {
		return nil, err
	}
	result := make([]*definition.Task, 0, len(arr))
	for _, obj := range arr {
		task, ok := obj.(*definition.Task)
		if !ok {
			continue
		}
		result = append(result, task)
	}
	return result, nil
}

func (s *Etcdv3Store) CreateTask(task *definition.Task) error {
	if task == nil {
		return errors.New("task should not be nil")
	}
	return s.create(s.keyTask(task.ID), task)
}

func (s *Etcdv3Store) UpdateTask(task *definition.Task) error {
	if task == nil {
		return errors.New("task should not be nil")
	}
	return s.update(s.keyTask(task.ID), task, true)
}

func (s *Etcdv3Store) RemoveTask(id string) error {
	return s.remove(s.keyTask(id), false)
}

func (s *Etcdv3Store) GetTaskRuntime(strategyId, taskId, id string) (*definition.TaskRuntime, error) {
	obj := &definition.TaskRuntime{}
	err := s.getObject(s.keyTaskRuntime(strategyId, taskId, id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv3Store) GetTaskRuntimes(strategyId, taskId string) ([]*definition.TaskRuntime, error) {
	arr, err := s.getObjects(s.keyTaskRuntimes(strategyId, taskId), reflect.TypeOf(definition.TaskRuntime{}))
	if err != nil {
		return nil, err
	}
	result := make([]*definition.TaskRuntime, 0, len(arr))
	for _, obj := range arr {
		r, ok := obj.(*definition.TaskRuntime)
		if !ok {
			continue
		}
		result = append(result, r)
	}
	return result, nil
}

func (s *Etcdv3Store) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	if runtime == nil {
		return errors.New("task runtime should not be nil")
	}
	return s.update(s.keyTaskRuntime(runtime.StrategyID, runtime.TaskID, runtime.ID), runtime, false)
}

func (s *Etcdv3Store) RemoveTaskRuntime(strategyId, taskId, id string) error {
	err := s.remove(s.keyTaskRuntime(strategyId, taskId, id), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv3Store) GetTaskItemsConfigVersion(strategyId, taskId string) (int64, error) {
	resp, err := s.kvApi.Get(context.Background(), s.keyTaskReload(strategyId, taskId))
	if err != nil {
		return 0, err
	}
	if resp.Count == 0 {
		return 0, nil
	}
	ver, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, err
	}
	return ver, nil
}

func (s *Etcdv3Store) IncreaseTaskItemsConfigVersion(strategyId, taskId string) error {
	key := s.keyTaskReload(strategyId, taskId)
	resp, err := s.kvApi.Txn(context.Background()).
		If(etcd.Compare(etcd.CreateRevision(key), "=", 0)).
		Then(etcd.OpPut(key, "1")).
		Else(etcd.OpGet(key)).
		Commit()
	if err != nil {
		return err
	}
	if resp.Succeeded {
		// initial
		return nil
	}
	get_resp := resp.Responses[0].GetResponseRange()
	if get_resp.Count < 1 {
		return errors.New("Key create failed")
	}
	for {
		cur, err := strconv.ParseInt(string(get_resp.Kvs[0].Value), 10, 64)
		if err != nil {
			cur = 0
		}
		resp, err = s.kvApi.Txn(context.Background()).
			If(etcd.Compare(etcd.Version(key), "=", get_resp.Kvs[0].Version)).
			Then(etcd.OpPut(key, strconv.FormatInt(cur+1, 10))).
			Else(etcd.OpGet(key)).
			Commit()
		if err != nil {
			return err
		}
		if resp.Succeeded {
			break
		}
		get_resp = resp.Responses[0].GetResponseRange()
	}
	return nil
}

func (s *Etcdv3Store) GetTaskAssignment(strategyId, taskId, itemId string) (*definition.TaskAssignment, error) {
	obj := &definition.TaskAssignment{}
	err := s.getObject(s.keyTaskAssignment(strategyId, taskId, itemId), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv3Store) GetTaskAssignments(strategyId, taskId string) ([]*definition.TaskAssignment, error) {
	arr, err := s.getObjects(s.keyTaskAssignments(strategyId, taskId), reflect.TypeOf(definition.TaskAssignment{}))
	if err != nil {
		return nil, err
	}
	result := make([]*definition.TaskAssignment, 0, len(arr))
	for _, obj := range arr {
		assign, ok := obj.(*definition.TaskAssignment)
		if !ok {
			continue
		}
		result = append(result, assign)
	}
	return result, nil
}

func (s *Etcdv3Store) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	if assignment == nil {
		return errors.New("assignment should not be nil")
	}
	return s.update(s.keyTaskAssignment(assignment.StrategyID, assignment.TaskID, assignment.ItemID), assignment, false)
}

func (s *Etcdv3Store) RemoveTaskAssignment(strategyId, taskId, itemId string) error {
	err := s.remove(s.keyTaskAssignment(strategyId, taskId, itemId), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv3Store) GetStrategy(id string) (*definition.Strategy, error) {
	obj := &definition.Strategy{}
	err := s.getObject(s.keyStrategy(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv3Store) GetStrategies() ([]*definition.Strategy, error) {
	arr, err := s.getObjects(s.keyStrategies(), reflect.TypeOf(definition.Strategy{}))
	if err != nil {
		return nil, err
	}
	result := make([]*definition.Strategy, 0, len(arr))
	for _, obj := range arr {
		strategy, ok := obj.(*definition.Strategy)
		if !ok {
			continue
		}
		result = append(result, strategy)
	}
	return result, nil
}

func (s *Etcdv3Store) CreateStrategy(strategy *definition.Strategy) error {
	if strategy == nil {
		return errors.New("strategy should not be nil")
	}
	return s.create(s.keyStrategy(strategy.ID), strategy)
}

func (s *Etcdv3Store) UpdateStrategy(strategy *definition.Strategy) error {
	if strategy == nil {
		return errors.New("strategy should not be nil")
	}
	return s.update(s.keyStrategy(strategy.ID), strategy, true)
}

func (s *Etcdv3Store) RemoveStrategy(id string) error {
	return s.remove(s.keyStrategy(id), false)
}

func (s *Etcdv3Store) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	obj := &definition.StrategyRuntime{}
	err := s.getObject(s.keyRuntime(strategyId, schedulerId), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv3Store) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
	arr, err := s.getObjects(s.keyRuntimes(strategyId), reflect.TypeOf(definition.StrategyRuntime{}))
	if err != nil {
		return nil, err
	}
	result := make([]*definition.StrategyRuntime, 0, len(arr))
	for _, obj := range arr {
		s, ok := obj.(*definition.StrategyRuntime)
		if !ok {
			continue
		}
		result = append(result, s)
	}
	return result, nil
}

func (s *Etcdv3Store) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	if runtime == nil {
		return errors.New("runtime should not be nil")
	}
	return s.update(s.keyRuntime(runtime.StrategyID, runtime.SchedulerID), runtime, false)
}

func (s *Etcdv3Store) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	err := s.remove(s.keyRuntime(strategyId, schedulerId), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv3Store) Dump() string {
	result, _ := s.getChildren(s.prefix, true)
	keys := make([]string, 0, len(result))
	for k := range result {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := strings.Builder{}
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(result[k])
		b.WriteString("\n")
	}
	return b.String()
}
