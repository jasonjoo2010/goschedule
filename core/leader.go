package core

import (
	"errors"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/core/worker/task_worker"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
)

func (s *ScheduleManager) isLeader(strategyId string) bool {
	list, err := s.store.GetStrategyRuntimes(strategyId)
	if err != nil || len(list) < 1 {
		return false
	}
	arr := make([]string, len(list))
	var myself *definition.StrategyRuntime
	for i, runtime := range list {
		arr[i] = runtime.SchedulerId
		if runtime.SchedulerId == s.scheduler.Id {
			myself = runtime
		}
	}
	now := time.Now().Unix() * 1000
	if myself != nil && now-myself.CreateAt < s.StallAfterStartup {
		// schedule after stalling
		return false
	}
	leaderUUID := utils.FetchLeader(arr)
	if leaderUUID == s.scheduler.Id {
		return true
	}
	// double check the scheduler really exists
	scheduler, _ := s.store.GetScheduler(leaderUUID)
	if scheduler == nil {
		// dirty runtime
		s.cleanScheduler(leaderUUID)
	}
	return false
}

func (s *ScheduleManager) cleanScheduler(schedulerId string) {
	s.store.UnregisterScheduler(schedulerId)
	// clean dead runtimes binded to it
	strategies, err := s.store.GetStrategies()
	if err != nil {
		logrus.Warn("Failed to fetch strategies: ", err.Error())
		return
	}
	for _, strategy := range strategies {
		s.store.RemoveStrategyRuntime(strategy.Id, schedulerId)
	}
}

func (s *ScheduleManager) clearExpiredSchedulers() {
	schedulers, err := s.store.GetSchedulers()
	if err != nil {
		logrus.Warn("Get scheudlers failed: ", err.Error())
		return
	}
	now := time.Now().UnixNano() / 1e6
	for _, scheduler := range schedulers {
		if now-scheduler.LastHeartbeat > s.DeathTimeout.Milliseconds() {
			logrus.Info("Clear expired scheduler: ", scheduler.Id, ", last reach at ", scheduler.LastHeartbeat)
			s.cleanScheduler(scheduler.Id)
		}
	}
}

func (s *ScheduleManager) generateRuntimes() {
	strategies, err := s.store.GetStrategies()
	if err != nil {
		logrus.Warn("Get strategies failed: ", err.Error())
		return
	}
	hostname := utils.GetHostName()
	ip := utils.GetHostIPv4()
	for _, strategy := range strategies {
		canSchedule := utils.CanSchedule(strategy.IpList, hostname, ip)
		runtime, err := s.store.GetStrategyRuntime(strategy.Id, s.scheduler.Id)
		if err != nil && err != store.NotExist {
			continue
		}
		if s.scheduler.Enabled && strategy.Enabled && canSchedule {
			// register runtimes if lack
			if runtime == nil {
				runtime = &definition.StrategyRuntime{
					SchedulerId: s.scheduler.Id,
					StrategyId:  strategy.Id,
					CreateAt:    time.Now().Unix() * 1000,
				}
				s.store.SetStrategyRuntime(runtime)
			}
		} else {
			// clear runtimes if any
			if runtime != nil {
				logrus.Info("Clean runtime for strategy: ", runtime.StrategyId, " with scheduler ", runtime.SchedulerId)
				s.store.RemoveStrategyRuntime(strategy.Id, s.scheduler.Id)
				// stop the workers
				s.stopWorkers(strategy)
			}
		}
	}
}

func (s *ScheduleManager) assign() {
	strategies, err := s.store.GetStrategies()
	if err != nil {
		logrus.Warn("Failed to fetch strategies", err)
		return
	}

	for _, strategy := range strategies {
		if !strategy.Enabled {
			continue
		}
		if !s.isLeader(strategy.Id) {
			continue
		}
		// It's the leader to specific strategy
		runtimes, err := s.store.GetStrategyRuntimes(strategy.Id)
		if err != nil {
			logrus.Warn("Failed to fetch runtimes for ", strategy.Id, ": ", err.Error())
			continue
		}
		workerRequiredArr := utils.AssignWorkers(len(runtimes), strategy.Total, strategy.MaxOnSingleScheduler)
		utils.SortRuntimesWithShuffle(runtimes)
		for i := 0; i < len(runtimes); i++ {
			if workerRequiredArr[i] != runtimes[i].RequestedNum {
				runtimes[i].RequestedNum = workerRequiredArr[i]
				s.store.SetStrategyRuntime(runtimes[i])
			}
		}
	}
}

func (s *ScheduleManager) createWorker(strategy *definition.Strategy) (worker.Worker, error) {
	switch strategy.Kind {
	case definition.SimpleKind:
		return worker.NewSimple(*strategy)
	case definition.FuncKind:
		return worker.NewFunc(*strategy)
	case definition.TaskKind:
		task, err := s.store.GetTask(strategy.Bind)
		if err != nil {
			return nil, err
		}
		return task_worker.NewTask(*strategy, *task, s.store, s.scheduler.Id)
	default:
		logrus.Error("Unknow Kind of strategy: ", strategy.Kind)
		return nil, errors.New("Unknow strategy kind")
	}
}

func (s *ScheduleManager) adjustWorkers() {
	strategies, err := s.store.GetStrategies()
	if err != nil {
		logrus.Warn("Failed to fetch strategies ", err)
		return
	}

	for _, strategy := range strategies {
		if !strategy.Enabled {
			continue
		}
		runtime, err := s.store.GetStrategyRuntime(strategy.Id, s.scheduler.Id)
		if err == store.NotExist {
			continue
		}
		if err != nil {
			logrus.Warn("Failed to fetch runtime for ", strategy.Id, ": ", err.Error())
			continue
		}
		if runtime.RequestedNum < 0 {
			logrus.Error("Requested count of workers in runtime is set to a wrong number: ", runtime.RequestedNum, " for ", strategy.Id)
			runtime.RequestedNum = 0
		}
		workers, ok := s.workersMap[runtime.StrategyId]
		if !ok {
			workers = make([]worker.Worker, 0, utils.Max(1, runtime.RequestedNum))
			s.workersMap[runtime.StrategyId] = workers
		}
		if len(workers) != runtime.RequestedNum {
			delta := runtime.RequestedNum - len(workers)
			if delta > 0 {
				// increase
				logrus.Info("Increase worker by ", delta, " for ", strategy.Id, " on ", s.scheduler.Id)
				for i := 0; i < delta; i++ {
					w, err := s.createWorker(strategy)
					if err != nil {
						logrus.Error("Can't create worker for: ", strategy.Id)
						continue
					}
					w.Start(strategy.Id, strategy.Parameter)
					logrus.Info("Worker of strategy ", strategy.Id, " started")
					workers = append(workers, w)
				}
			} else {
				// decrease
				logrus.Info("Decrease worker by ", -delta, " for ", strategy.Id, " on ", s.scheduler.Id)
				discards := workers[len(workers)-utils.Abs(delta):]
				workers = workers[:len(workers)-utils.Abs(delta)]
				// stop them
				for _, w := range discards {
					w.Stop(strategy.Id, strategy.Parameter)
				}
			}
			s.workersMap[runtime.StrategyId] = workers
		}
		// update info in storage
		if runtime.Num != len(workers) {
			runtime.Num = len(workers)
			s.store.SetStrategyRuntime(runtime)
		}
	}
}

func (s *ScheduleManager) schedule() {
	s.clearExpiredSchedulers()
	s.generateRuntimes()
	// calculate schedule table
	s.assign()
	// adjust local workers
	s.adjustWorkers()
}

// stopWorkers stop group of workers binded to specific strategy
func (s *ScheduleManager) stopWorkers(strategy *definition.Strategy) {
	workers, ok := s.workersMap[strategy.Id]
	if false == ok {
		return
	}
	for _, w := range workers {
		w.Stop(strategy.Id, strategy.Parameter)
	}
	delete(s.workersMap, strategy.Id)
}

func (s *ScheduleManager) stopAllWorkers() {
	for k := range s.workersMap {
		strategy, _ := s.store.GetStrategy(k)
		if strategy == nil {
			logrus.Warn("Strategy not found: ", k)
			s.stopWorkers(&definition.Strategy{
				Id:        k,
				Parameter: "",
			})
		} else {
			s.stopWorkers(strategy)
		}
	}
}

func (s *ScheduleManager) scheduleLoop() {
	// stop handler
	defer func() { s.shutdownNotifier <- 2 }()
	for !s.needStop {
		s.schedule()
		utils.Delay(s, s.ScheduleInterval)
	}
	s.stopAllWorkers()
}
