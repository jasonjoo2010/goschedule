// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/jasonjoo2010/godao"
	"github.com/jasonjoo2010/godao/options"
	"github.com/jasonjoo2010/godao/types"
	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/sirupsen/logrus"
)

type DatabaseStore struct {
	db        *sql.DB
	dao       *godao.Dao
	namespace string
}

func (s *DatabaseStore) keySequence() string {
	return s.namespace + "/sequence"
}

func (s *DatabaseStore) keyTasks() string {
	return s.namespace + "/tasks"
}

func (s *DatabaseStore) keyTask(id string) string {
	return s.keyTasks() + "/" + id
}

func (s *DatabaseStore) keyStrategies() string {
	return s.namespace + "/strategies"
}

func (s *DatabaseStore) keyStrategy(id string) string {
	return s.keyStrategies() + "/" + id
}

func (s *DatabaseStore) keySchedulers() string {
	return s.namespace + "/schedulers"
}

func (s *DatabaseStore) keyScheduler(id string) string {
	return s.keySchedulers() + "/" + id
}

func (s *DatabaseStore) keyRuntime(strategyId, schedulerId string) string {
	return s.keyRuntimes(strategyId) + "/" + schedulerId
}

func (s *DatabaseStore) keyRuntimes(strategyId string) string {
	return s.namespace + "/runtimes/" + strategyId
}

func (s *DatabaseStore) keyTaskRuntime(strategyId, taskId, runtimeId string) string {
	return s.keyTaskRuntimes(strategyId, taskId) + "/" + runtimeId
}

func (s *DatabaseStore) keyTaskRuntimes(strategyId, taskId string) string {
	return s.namespace + "/taskRuntimes/" + strategyId + "/" + taskId
}

func (s *DatabaseStore) keyTaskAssignment(strategyId, taskId, itemId string) string {
	return s.keyTaskAssignments(strategyId, taskId) + "/" + itemId
}

func (s *DatabaseStore) keyTaskAssignments(strategyId, taskId string) string {
	return s.namespace + "/taskAssignments/" + strategyId + "/" + taskId
}

func (s *DatabaseStore) keyTaskReload(strategyId, taskId string) string {
	return s.namespace + "/taskReload/" + strategyId + "/" + taskId
}

func (s *DatabaseStore) getObject(key string, obj interface{}) error {
	o, err := s.dao.SelectOneBy(context.Background(), "Key", key)
	if err != nil {
		return err
	}
	if o == nil {
		return store.NotExist
	}
	info := o.(*ScheduleInfo)
	return json.Unmarshal([]byte(info.Value), obj)
}

func (s *DatabaseStore) getObjects(base_key string, t reflect.Type) ([]interface{}, error) {
	list, err := s.dao.Select(context.Background(),
		(&godao.Query{}).
			StartsWith("Key", strings.TrimRight(base_key, "/")+"/").
			Limit(2000).
			Data(),
	)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	result := make([]interface{}, 0, len(list))
	for _, o := range list {
		info := o.(*ScheduleInfo)
		obj := reflect.New(t).Interface()
		err = json.Unmarshal([]byte(info.Value), obj)
		if err != nil {
			logrus.Warn("Wrong data type during deserializing: ", info.Key)
			continue
		}
		result = append(result, obj)
	}
	return result, nil
}

func (s *DatabaseStore) Name() string {
	return "database"
}

func (s *DatabaseStore) Time() int64 {
	return time.Now().UnixNano() / 1e6
}

func (s *DatabaseStore) Sequence() (uint64, error) {
	key := s.keySequence()
	// Actually we can use transaction here
	// But now pick optimistic update way
	retries := 10
	for retries > 0 {
		obj, err := s.dao.SelectOneBy(context.Background(), "Key", key)
		if err != nil {
			logrus.Warn("Failed to fetch info from database: ", err.Error())
			return 0, err
		}
		if obj == nil {
			// first time, insert
			_, _, err := s.dao.Insert(context.Background(), ScheduleInfo{
				Key:     key,
				Version: 1,
			}, options.WithInsertIgnore())
			if err != nil {
				return 0, err
			}
			continue
		}
		info := obj.(*ScheduleInfo)
		affected, err := s.dao.UpdateBy(context.Background(), (&godao.Query{}).
			Equal("Key", key).
			Equal("Version", info.Version).
			Data(),
			types.NewIncrease("Version", 1),
		)
		if err != nil {
			return 0, err
		}
		if affected > 0 {
			// succ
			return uint64(info.Version) + 1, nil
		}
		// retry
		retries--
	}
	return 0, errors.New("Failed to get a valid sequence")
}

func (s *DatabaseStore) Close() error {
	// nothing to do
	return nil
}

func (s *DatabaseStore) RegisterScheduler(scheduler *definition.Scheduler) error {
	if scheduler == nil {
		return errors.New("scheduler should not be nil")
	}
	return s.updateOrInsert(s.keyScheduler(scheduler.Id), scheduler)
}
func (s *DatabaseStore) UnregisterScheduler(id string) error {
	err := s.remove(s.keyScheduler(id))
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}
func (s *DatabaseStore) GetSchedulers() ([]*definition.Scheduler, error) {
	arr, err := s.getObjects(s.keySchedulers(), reflect.TypeOf(definition.Scheduler{}))
	if err != nil {
		return nil, err
	}
	if len(arr) == 0 {
		return []*definition.Scheduler{}, nil
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
func (s *DatabaseStore) GetScheduler(id string) (*definition.Scheduler, error) {
	obj := &definition.Scheduler{}
	err := s.getObject(s.keyScheduler(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *DatabaseStore) GetTask(id string) (*definition.Task, error) {
	obj := &definition.Task{}
	err := s.getObject(s.keyTask(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
func (s *DatabaseStore) GetTasks() ([]*definition.Task, error) {
	arr, err := s.getObjects(s.keyTasks(), reflect.TypeOf(definition.Task{}))
	if err != nil {
		return nil, err
	}
	if len(arr) == 0 {
		return []*definition.Task{}, nil
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
func (s *DatabaseStore) CreateTask(task *definition.Task) error {
	if task == nil {
		return errors.New("task should not be nil")
	}
	return s.create(s.keyTask(task.Id), task)
}
func (s *DatabaseStore) UpdateTask(task *definition.Task) error {
	if task == nil {
		return errors.New("task should not be nil")
	}
	return s.update(s.keyTask(task.Id), task)
}
func (s *DatabaseStore) RemoveTask(id string) error {
	return s.remove(s.keyTask(id))
}

func (s *DatabaseStore) GetTaskRuntime(strategyId, taskId, id string) (*definition.TaskRuntime, error) {
	obj := &definition.TaskRuntime{}
	err := s.getObject(s.keyTaskRuntime(strategyId, taskId, id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
func (s *DatabaseStore) GetTaskRuntimes(strategyId, taskId string) ([]*definition.TaskRuntime, error) {
	arr, err := s.getObjects(s.keyTaskRuntimes(strategyId, taskId), reflect.TypeOf(definition.TaskRuntime{}))
	if err != nil {
		return nil, err
	}
	if len(arr) == 0 {
		return []*definition.TaskRuntime{}, nil
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
func (s *DatabaseStore) SetTaskRuntime(runtime *definition.TaskRuntime) error {
	if runtime == nil {
		return errors.New("task runtime should not be nil")
	}
	return s.updateOrInsert(s.keyTaskRuntime(runtime.StrategyId, runtime.TaskId, runtime.Id), runtime)
}
func (s *DatabaseStore) RemoveTaskRuntime(strategyId, taskId, id string) error {
	err := s.remove(s.keyTaskRuntime(strategyId, taskId, id))
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *DatabaseStore) GetTaskItemsConfigVersion(strategyId, taskId string) (int64, error) {
	key := s.keyTaskReload(strategyId, taskId)
	obj, err := s.dao.SelectOneBy(context.Background(), "Key", key)
	if err != nil {
		return 0, err
	}
	if obj == nil {
		return 0, nil
	}
	info := obj.(*ScheduleInfo)
	return info.Version, nil
}
func (s *DatabaseStore) IncreaseTaskItemsConfigVersion(strategyId, taskId string) error {
	key := s.keyTaskReload(strategyId, taskId)
	for {
		affected, err := s.dao.UpdateBy(context.Background(), (&godao.Query{}).
			Equal("Key", key).
			Data(),
			types.NewIncrease("Version", 1),
		)
		if err != nil {
			return err
		}
		if affected > 0 {
			break
		}
		// initial
		s.dao.Insert(context.Background(),
			ScheduleInfo{
				Key: key,
			},
			options.WithInsertIgnore(),
		)
	}
	return nil
}

func (s *DatabaseStore) GetTaskAssignment(strategyId, taskId, itemId string) (*definition.TaskAssignment, error) {
	obj := &definition.TaskAssignment{}
	err := s.getObject(s.keyTaskAssignment(strategyId, taskId, itemId), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
func (s *DatabaseStore) GetTaskAssignments(strategyId, taskId string) ([]*definition.TaskAssignment, error) {
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
func (s *DatabaseStore) SetTaskAssignment(assignment *definition.TaskAssignment) error {
	if assignment == nil {
		return errors.New("assignment should not be nil")
	}
	return s.updateOrInsert(s.keyTaskAssignment(assignment.StrategyId, assignment.TaskId, assignment.ItemId), assignment)
}
func (s *DatabaseStore) RemoveTaskAssignment(strategyId, taskId, itemId string) error {
	err := s.remove(s.keyTaskAssignment(strategyId, taskId, itemId))
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *DatabaseStore) GetStrategy(id string) (*definition.Strategy, error) {
	obj := &definition.Strategy{}
	err := s.getObject(s.keyStrategy(id), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
func (s *DatabaseStore) GetStrategies() ([]*definition.Strategy, error) {
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
func (s *DatabaseStore) CreateStrategy(strategy *definition.Strategy) error {
	if strategy == nil {
		return errors.New("strategy should not be nil")
	}
	return s.create(s.keyStrategy(strategy.Id), strategy)
}
func (s *DatabaseStore) UpdateStrategy(strategy *definition.Strategy) error {
	if strategy == nil {
		return errors.New("strategy should not be nil")
	}
	return s.update(s.keyStrategy(strategy.Id), strategy)
}
func (s *DatabaseStore) RemoveStrategy(id string) error {
	return s.remove(s.keyStrategy(id))
}

func (s *DatabaseStore) GetStrategyRuntime(strategyId, schedulerId string) (*definition.StrategyRuntime, error) {
	obj := &definition.StrategyRuntime{}
	err := s.getObject(s.keyRuntime(strategyId, schedulerId), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
func (s *DatabaseStore) GetStrategyRuntimes(strategyId string) ([]*definition.StrategyRuntime, error) {
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
func (s *DatabaseStore) SetStrategyRuntime(runtime *definition.StrategyRuntime) error {
	if runtime == nil {
		return errors.New("runtime should not be nil")
	}
	return s.updateOrInsert(s.keyRuntime(runtime.StrategyId, runtime.SchedulerId), runtime)
}
func (s *DatabaseStore) RemoveStrategyRuntime(strategyId, schedulerId string) error {
	err := s.remove(s.keyRuntime(strategyId, schedulerId))
	// ignore not exist
	if err == store.NotExist {
		return nil
	}
	return err
}

func (s *DatabaseStore) Dump() string {
	page := 1
	size := 50
	condition := &godao.Query{}
	condition.StartsWith("Key", s.namespace+"/").OrderBy("Key", false)
	b := strings.Builder{}
	for {
		condition.Page(page, size)
		list, err := s.dao.Select(context.Background(), condition.Data())
		if err != nil {
			logrus.Warn("Error occurred when dumping: ", err.Error())
			break
		}
		if len(list) < 1 {
			break
		}
		for _, obj := range list {
			item := obj.(*ScheduleInfo)
			b.WriteString(item.Key)
			b.WriteString(": ")
			b.WriteString(item.Value)
			b.WriteString("\n")
		}
		page++
	}
	return b.String()
}
