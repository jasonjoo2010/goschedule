// Copyright 2020 The GoSchedule Authors. All rights reserved.
// Use of this source code is governed by BSD
// license that can be found in the LICENSE file.

package core

import (
	"errors"
	"sync"
	"time"

	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/core/worker/task_worker"
	"github.com/jasonjoo2010/goschedule/definition"
	"github.com/jasonjoo2010/goschedule/log"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/types"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
)

func (manager *ScheduleManager) isLeader(strategyId string) bool {
	list, err := manager.store.GetStrategyRuntimes(strategyId)
	if err != nil || len(list) < 1 {
		return false
	}
	arr := make([]string, len(list))
	var myself *definition.StrategyRuntime
	for i, runtime := range list {
		arr[i] = runtime.SchedulerID
		if runtime.SchedulerID == manager.scheduler.ID {
			myself = runtime
		}
	}
	now := time.Now().Unix() * 1000
	if myself != nil && now-myself.CreateAt < manager.cfg.StallAfterStartup.Milliseconds() {
		// schedule after stalling
		return false
	}
	leaderUUID := utils.FetchLeader(arr)
	if leaderUUID == manager.scheduler.ID {
		return true
	}
	// double check the scheduler really exists
	scheduler, _ := manager.store.GetScheduler(leaderUUID)
	if scheduler == nil {
		// dirty runtime
		manager.cleanScheduler(leaderUUID)
	}
	return false
}

func (manager *ScheduleManager) cleanScheduler(schedulerId string) {
	manager.store.UnregisterScheduler(schedulerId)
	// clean dead runtimes binded to it
	strategies, err := manager.store.GetStrategies()
	if err != nil {
		log.Warnf("Failed to fetch strategies: %s", err.Error())
		return
	}
	for _, strategy := range strategies {
		manager.store.RemoveStrategyRuntime(strategy.ID, schedulerId)
	}
}

func (manager *ScheduleManager) clearExpiredSchedulers() {
	schedulers, err := manager.store.GetSchedulers()
	if err != nil {
		logrus.Warnf("Get scheudlers failed: %s", err.Error())
		return
	}
	now := time.Now().UnixNano() / 1e6
	for _, scheduler := range schedulers {
		if now-scheduler.LastHeartbeat > manager.cfg.DeathTimeout.Milliseconds() {
			logrus.Info("Clear expired scheduler: ", scheduler.ID, ", last reach at ", scheduler.LastHeartbeat)
			manager.cleanScheduler(scheduler.ID)
		}
	}
}

func (manager *ScheduleManager) generateRuntimes() {
	strategies, err := manager.store.GetStrategies()
	if err != nil {
		logrus.Warn("Get strategies failed: ", err.Error())
		return
	}
	hostname := utils.GetHostName()
	ip := utils.GetHostIPv4()
	for _, strategy := range strategies {
		canSchedule := utils.CanSchedule(strategy.IPList, hostname, ip)
		runtime, err := manager.store.GetStrategyRuntime(strategy.ID, manager.scheduler.ID)
		if err != nil && err != store.NotExist {
			continue
		}
		if manager.scheduler.Enabled && strategy.Enabled && canSchedule {
			// register runtimes if lack
			if runtime == nil {
				runtime = &definition.StrategyRuntime{
					SchedulerID: manager.scheduler.ID,
					StrategyID:  strategy.ID,
					CreateAt:    time.Now().Unix() * 1000,
				}
				manager.store.SetStrategyRuntime(runtime)
			}
		} else {
			// clear runtimes if any
			if runtime != nil {
				logrus.Info("Clean runtime for strategy: ", runtime.StrategyID, " with scheduler ", runtime.SchedulerID)
				manager.store.RemoveStrategyRuntime(strategy.ID, manager.scheduler.ID)
				// stop the workers
				manager.stopWorkers(strategy)
			}
		}
	}
}

func (manager *ScheduleManager) assign() {
	strategies, err := manager.store.GetStrategies()
	if err != nil {
		logrus.Warn("Failed to fetch strategies", err)
		return
	}

	for _, strategy := range strategies {
		if !strategy.Enabled {
			continue
		}
		if !manager.isLeader(strategy.ID) {
			continue
		}
		// It's the leader to specific strategy
		runtimes, err := manager.store.GetStrategyRuntimes(strategy.ID)
		if err != nil {
			logrus.Warn("Failed to fetch runtimes for ", strategy.ID, ": ", err.Error())
			continue
		}
		workerRequiredArr := utils.AssignWorkers(len(runtimes), strategy.Total, strategy.MaxOnSingleScheduler)
		utils.SortRuntimesWithShuffle(runtimes)
		for i := 0; i < len(runtimes); i++ {
			if workerRequiredArr[i] != runtimes[i].RequestedNum {
				runtimes[i].RequestedNum = workerRequiredArr[i]
				manager.store.SetStrategyRuntime(runtimes[i])
			}
		}
	}
}

func (manager *ScheduleManager) createWorker(strategy *definition.Strategy) (types.Worker, error) {
	switch strategy.Kind {
	case definition.SimpleKind:
		return worker.NewSimple(*strategy)
	case definition.FuncKind:
		return worker.NewFunc(*strategy)
	case definition.TaskKind:
		task, err := manager.store.GetTask(strategy.Bind)
		if err != nil {
			return nil, err
		}
		return task_worker.NewTask(*strategy, *task, manager.store, manager.scheduler.ID)
	default:
		logrus.Error("Unknow Kind of strategy: ", strategy.Kind)
		return nil, errors.New("Unknow strategy kind")
	}
}

func (manager *ScheduleManager) maintainWorkers(strategy *definition.Strategy, target int) {
	workersCnt := manager.workerSet.WorkersCountFor(strategy.ID)
	delta := target - workersCnt
	if delta > 0 {
		// increase
		log.Infof("Increase worker by %d for %s on %s", delta, strategy.ID, manager.scheduler.ID)
		for i := 0; i < delta; i++ {
			w, err := manager.createWorker(strategy)
			if err != nil {
				logrus.Error("Can't create worker for: ", strategy.ID)
				continue
			}
			go w.Start(strategy.ID, strategy.Parameter)
			logrus.Info("Worker of strategy ", strategy.ID, " started")
			manager.workerSet.AddWorker(strategy.ID, w)
		}
	} else if delta < 0 {
		// decrease
		logrus.Info("Decrease worker by ", -delta, " for ", strategy.ID, " on ", manager.scheduler.ID)
		for i := 0; i < -delta; i++ {
			if w := manager.workerSet.RemoveWorker(strategy.ID); w != nil {
				go w.Stop(strategy.ID, strategy.Parameter)
			}
		}
	}
}

func (manager *ScheduleManager) adjustWorkers() {
	strategies, err := manager.store.GetStrategies()
	if err != nil {
		logrus.Warn("Failed to fetch strategies ", err)
		return
	}

	for _, strategy := range strategies {
		if !strategy.Enabled {
			continue
		}
		runtime, err := manager.store.GetStrategyRuntime(strategy.ID, manager.scheduler.ID)
		if err == store.NotExist {
			continue
		}
		if err != nil {
			logrus.Warn("Failed to fetch runtime for ", strategy.ID, ": ", err.Error())
			continue
		}
		if runtime.RequestedNum < 0 {
			logrus.Error("Requested count of workers in runtime is set to a wrong number: ", runtime.RequestedNum, " for ", strategy.ID)
			runtime.RequestedNum = 0
		}
		workersCnt := manager.workerSet.WorkersCountFor(runtime.StrategyID)
		if workersCnt != runtime.RequestedNum {
			manager.maintainWorkers(strategy, runtime.RequestedNum)
			workersCnt = manager.workerSet.WorkersCountFor(runtime.StrategyID)
		}
		// update info in storage
		if runtime.Num != workersCnt {
			runtime.Num = workersCnt
			manager.store.SetStrategyRuntime(runtime)
		}
	}
}

func (manager *ScheduleManager) schedule() {
	manager.clearExpiredSchedulers()
	manager.generateRuntimes()
	// calculate schedule table
	manager.assign()
	// adjust local workers
	manager.adjustWorkers()
}

// stopWorkers stop group of workers binded to specific strategy
func (manager *ScheduleManager) stopWorkers(strategy *definition.Strategy) error {
	defer manager.workerSet.Delete(strategy.ID)

	workers := manager.workerSet.WorkersFor(strategy.ID)
	if len(workers) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	for _, w := range workers {
		wg.Add(1)
		go func(w types.Worker) {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("Stop worker %s failed: %v", strategy.ID, err)
				}
			}()
			w.Stop(strategy.ID, strategy.Parameter)
		}(w)
	}

	wg.Wait()
	return nil
}

func (manager *ScheduleManager) stopAllWorkers() {
	wg := sync.WaitGroup{}
	names := manager.workerSet.Strategies()
	for _, name := range names {
		strategy, _ := manager.store.GetStrategy(name)
		if strategy == nil {
			logrus.Warn("Strategy not found: ", name)
			strategy = &definition.Strategy{
				ID:        name,
				Parameter: "",
			}
		}

		wg.Add(1)
		go func(s *definition.Strategy) {
			defer wg.Done()
			manager.stopWorkers(s)
		}(strategy)
	}
	// wait
	wg.Wait()
}
