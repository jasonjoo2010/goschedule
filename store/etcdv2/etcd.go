// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package etcdv2

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/client"
	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/sirupsen/logrus"
)

type Etcdv2Store struct {
	client    etcd.Client
	keysApi   etcd.KeysAPI
	prefix    string
	timeDelta time.Duration
}

func (s *Etcdv2Store) keySequenceBase() string {
	return s.prefix + "/sequence"
}

func (s *Etcdv2Store) keyTasks() string {
	return s.prefix + "/tasks"
}

func (s *Etcdv2Store) keyTask(id string) string {
	return s.keyTasks() + "/" + id
}

func (s *Etcdv2Store) keyStrategies() string {
	return s.prefix + "/strategies"
}

func (s *Etcdv2Store) keyStrategy(id string) string {
	return s.keyStrategies() + "/" + id
}

func (s *Etcdv2Store) keySchedulers() string {
	return s.prefix + "/schedulers"
}

func (s *Etcdv2Store) keyScheduler(id string) string {
	return s.keySchedulers() + "/" + id
}

func (s *Etcdv2Store) keyRuntime(strategyId, schedulerId string) string {
	return s.keyRuntimes(strategyId) + "/" + schedulerId
}

func (s *Etcdv2Store) keyRuntimes(strategyId string) string {
	return s.prefix + "/runtimes/" + strategyId
}

func (s *Etcdv2Store) keyTaskRuntime(strategyId, taskId, runtimeId string) string {
	return s.keyTaskRuntimes(strategyId, taskId) + "/" + runtimeId
}

func (s *Etcdv2Store) keyTaskRuntimes(strategyId, taskId string) string {
	return s.prefix + "/taskRuntimes/" + strategyId + "/" + taskId
}

func (s *Etcdv2Store) keyTaskAssignment(strategyId, taskId, itemId string) string {
	return s.keyTaskAssignments(strategyId, taskId) + "/" + itemId
}

func (s *Etcdv2Store) keyTaskAssignments(strategyId, taskId string) string {
	return s.prefix + "/taskAssignments/" + strategyId + "/" + taskId
}

func (s *Etcdv2Store) keyTaskReload(strategyId, taskId string) string {
	return s.prefix + "/taskReload/" + strategyId + "/" + taskId
}

func (s *Etcdv2Store) getObject(key string, obj interface{}) error {
	resp, err := s.keysApi.Get(context.Background(), key, nil)
	if err != nil {
		return convertError(err)
	}
	if resp.Node.Dir {
		return errors.New("illegal data structure")
	}
	str := resp.Node.Value
	if str == "" {
		return store.NotExist
	}
	return json.Unmarshal([]byte(str), obj)
}

func (s *Etcdv2Store) getObjects(key string, t reflect.Type) ([]interface{}, error) {
	resp, err := s.keysApi.Get(context.Background(), key, nil)
	if err != nil {
		return nil, convertError(err)
	}
	if !resp.Node.Dir {
		return nil, errors.New("illegal data structure")
	}
	result := make([]interface{}, 0, len(resp.Node.Nodes))
	for _, n := range resp.Node.Nodes {
		obj := reflect.New(t).Interface()
		err = json.Unmarshal([]byte(n.Value), obj)
		if err != nil {
			logrus.Warn("Wrong data type during deserializing: " + n.Key)
			continue
		}
		result = append(result, obj)
	}
	return result, nil
}

func (s *Etcdv2Store) Name() string {
	return "etcdv2"
}

func (s *Etcdv2Store) Time() int64 {
	return time.Now().Add(s.timeDelta).UnixNano() / 1e6
}

func (s *Etcdv2Store) Sequence() (uint64, error) {
	ctx := context.Background()
	resp, err := s.keysApi.CreateInOrder(ctx, s.keySequenceBase(), "", &etcd.CreateInOrderOptions{
		TTL: 10 * time.Second,
	})
	if err != nil {
		return 0, err
	}
	key := resp.Node.Key
	resp, err = s.keysApi.Delete(ctx, key, nil)
	pos := strings.LastIndexByte(key, '/')
	if pos < 1 {
		return 0, errors.New("Got an illegal inorder key: " + key)
	}
	seq, err := strconv.ParseUint(key[pos+1:], 10, 64)
	if err != nil {
		return 0, errors.New("Got an illegal sequence key: " + key)
	}
	return seq, nil
}

func (s *Etcdv2Store) Close() error {
	return nil
}

func (s *Etcdv2Store) RegisterScheduler(scheduler *definition.Scheduler) error {
	if scheduler == nil {
		return errors.New("scheduler should not be nil")
	}
	return s.update(s.keyScheduler(scheduler.Id), scheduler, false)
}

func (s *Etcdv2Store) UnregisterScheduler(id string) error {
	err := s.remove(s.keyScheduler(id), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv2Store) GetSchedulers() ([]*definition.Scheduler, error) {
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

func (s *Etcdv2Store) GetScheduler(id string) (*definition.Scheduler, error) {
	obj := &definition.Scheduler{}
	err := s.getObject(s.keyScheduler(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv2Store) GetTask(id string) (*definition.Task, error) {
	obj := &definition.Task{}
	err := s.getObject(s.keyTask(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv2Store) GetTasks() ([]*definition.Task, error) {
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

func (s *Etcdv2Store) CreateTask(task *definition.Task) error {
	if task == nil {
		return errors.New("task should not be nil")
	}
	return s.create(s.keyTask(task.Id), task)
}

func (s *Etcdv2Store) UpdateTask(task *definition.Task) error {
	if task == nil {
		return errors.New("task should not be nil")
	}
	return s.update(s.keyTask(task.Id), task, true)
}

func (s *Etcdv2Store) RemoveTask(id string) error {
	return s.remove(s.keyTask(id), false)
}

func (s *Etcdv2Store) GetTaskRuntime(strategyId, taskId, id string) (*definition.TaskRuntime, error) {
	obj := &definition.TaskRuntime{}
	err := s.getObject(s.keyTaskRuntime(strategyId, taskId, id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv2Store) GetTaskRuntimes(strategyId, taskId string) ([]*definition.TaskRuntime, error) {
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

func (s *Etcdv2Store) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	if runtime == nil {
		return errors.New("task runtime should not be nil")
	}
	return s.update(s.keyTaskRuntime(runtime.StrategyId, runtime.TaskId, runtime.Id), runtime, false)
}

func (s *Etcdv2Store) RemoveTaskRuntime(strategyId, taskId, id string) error {
	err := s.remove(s.keyTaskRuntime(strategyId, taskId, id), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv2Store) GetTaskItemsConfigVersion(strategyId, taskId string) (int64, error) {
	resp, err := s.keysApi.Get(context.Background(), s.keyTaskReload(strategyId, taskId), nil)
	if err != nil {
		err = convertError(err)
		if err == store.NotExist {
			return 0, nil
		}
		return 0, err
	}
	ver, err := strconv.ParseInt(resp.Node.Value, 10, 64)
	if err != nil {
		return 0, err
	}
	return ver, nil
}

func (s *Etcdv2Store) IncreaseTaskItemsConfigVersion(strategyId, taskId string) error {
	ctx := context.Background()
	key := s.keyTaskReload(strategyId, taskId)
	opt := &etcd.SetOptions{}
	resp, err := s.keysApi.Get(ctx, key, nil)
	ver := int64(0)
	if err != nil {
		err = convertError(err)
		if err != store.NotExist {
			return err
		}
	} else {
		opt.PrevIndex = resp.Node.ModifiedIndex
		ver, _ = strconv.ParseInt(resp.Node.Value, 10, 64)
	}
	ver++
	_, err = s.keysApi.Set(ctx, key, strconv.FormatInt(ver, 10), opt)
	return err
}

func (s *Etcdv2Store) GetTaskAssignment(strategyId, taskId, itemId string) (*definition.TaskAssignment, error) {
	obj := &definition.TaskAssignment{}
	err := s.getObject(s.keyTaskAssignment(strategyId, taskId, itemId), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv2Store) GetTaskAssignments(strategyId, taskId string) ([]*definition.TaskAssignment, error) {
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

func (s *Etcdv2Store) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	if assignment == nil {
		return errors.New("assignment should not be nil")
	}
	return s.update(s.keyTaskAssignment(assignment.StrategyId, assignment.TaskId, assignment.ItemId), assignment, false)
}

func (s *Etcdv2Store) RemoveTaskAssignment(strategyId, taskId, itemId string) error {
	err := s.remove(s.keyTaskAssignment(strategyId, taskId, itemId), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv2Store) GetStrategy(id string) (*definition.Strategy, error) {
	obj := &definition.Strategy{}
	err := s.getObject(s.keyStrategy(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv2Store) GetStrategies() ([]*definition.Strategy, error) {
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

func (s *Etcdv2Store) CreateStrategy(strategy *definition.Strategy) error {
	if strategy == nil {
		return errors.New("strategy should not be nil")
	}
	return s.create(s.keyStrategy(strategy.Id), strategy)
}

func (s *Etcdv2Store) UpdateStrategy(strategy *definition.Strategy) error {
	if strategy == nil {
		return errors.New("strategy should not be nil")
	}
	return s.update(s.keyStrategy(strategy.Id), strategy, true)
}

func (s *Etcdv2Store) RemoveStrategy(id string) error {
	return s.remove(s.keyStrategy(id), false)
}

func (s *Etcdv2Store) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	obj := &definition.StrategyRuntime{}
	err := s.getObject(s.keyRuntime(strategyId, schedulerId), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *Etcdv2Store) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
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

func (s *Etcdv2Store) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	if runtime == nil {
		return errors.New("runtime should not be nil")
	}
	return s.update(s.keyRuntime(runtime.StrategyId, runtime.SchedulerId), runtime, false)
}

func (s *Etcdv2Store) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	err := s.remove(s.keyRuntime(strategyId, schedulerId), false)
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *Etcdv2Store) Dump() string {
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
