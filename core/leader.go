package core

import (
	"errors"
	"log"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/core/worker"
	"github.com/jasonjoo2010/goschedule/store"
	"github.com/jasonjoo2010/goschedule/utils"
)

func (s *ScheduleManager) isLeader(strategyId string) bool {
	list, err := s.store.GetStrategyRuntimes(strategyId)
	if err != nil || len(list) < 1 {
		return false
	}
	arr := make([]string, len(list))
	for i, runtime := range list {
		arr[i] = runtime.SchedulerId
	}
	return utils.IsLeader(arr, s.scheduler.Id)
}

func (s *ScheduleManager) cleanDeadScheduler(schedulerId string) {
	s.store.UnregisterScheduler(schedulerId)
	// clean dead runtimes binded to it
	strategies, err := s.store.GetStrategies()
	if err != nil {
		log.Println("Failed to fetch strategies", err)
		return
	}
	for _, strategy := range strategies {
		s.store.RemoveStrategyRuntime(strategy.Id, schedulerId)
	}
}

func (s *ScheduleManager) clearExpiredSchedulers() {
	schedulers, err := s.store.GetSchedulers()
	if err != nil {
		log.Println("Get scheudlers failed:", err)
		return
	}
	now := time.Now().UnixNano() / 1e6
	for _, scheduler := range schedulers {
		if now-scheduler.LastHeartbeat > s.deathTimeout.Milliseconds() {
			log.Println("Clear expired scheduler:", scheduler.Id, ", last reach at", scheduler.LastHeartbeat)
			s.cleanDeadScheduler(scheduler.Id)
		}
	}
}

func (s *ScheduleManager) generateRuntimes() {
	strategies, err := s.store.GetStrategies()
	if err != nil {
		log.Println("Get strategies failed:", err)
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
		if s.scheduler.Enabled && canSchedule {
			// register runtimes if lack
			if runtime == nil {
				runtime = &definition.StrategyRuntime{
					SchedulerId: s.scheduler.Id,
					StrategyId:  strategy.Id,
				}
				s.store.SetStrategyRuntime(runtime)
			}
		} else {
			// clear runtimes if any
			if runtime != nil {
				s.store.RemoveStrategyRuntime(strategy.Id, s.scheduler.Id)
				// stop the workers
				s.stopWorkers(strategy.Id)
			}
		}
	}
}

func (s *ScheduleManager) assign() {
	strategies, err := s.store.GetStrategies()
	if err != nil {
		log.Println("Failed to fetch strategies", err)
		return
	}

	for _, strategy := range strategies {
		if false == strategy.Enabled {
			// XXX strategy is disabled
		}
		if !s.isLeader(strategy.Id) {
			continue
		}
		// It's the leader to specific strategy
		runtimes, err := s.store.GetStrategyRuntimes(strategy.Id)
		if err != nil {
			log.Println("Failed to fetch runtimes for", strategy.Id, err)
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
	default:
		log.Fatal("Unknow Kind of strategy:", strategy.Kind)
		return nil, errors.New("Unknow strategy kind")
	}
}

func (s *ScheduleManager) adjustWorkers() {
	strategies, err := s.store.GetStrategies()
	if err != nil {
		log.Println("Failed to fetch strategies", err)
		return
	}

	for _, strategy := range strategies {
		runtime, err := s.store.GetStrategyRuntime(strategy.Id, s.scheduler.Id)
		if err != nil {
			log.Println("Failed to fetch runtime for", strategy.Id, err)
			continue
		}
		if runtime.RequestedNum < 0 {
			log.Fatal("Requested count of workers in runtime is set to a wrong number:", runtime.RequestedNum, "for", strategy.Id)
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
				log.Println("Increase worker by", delta, "for", strategy.Id, "on", s.scheduler.Id)
				for i := 0; i < delta; i++ {
					w, err := s.createWorker(strategy)
					if err != nil {
						log.Fatal("Can't create worker for:", strategy.Id)
						continue
					}
					workers = append(workers, w)
				}
			} else {
				// decrease
				log.Println("Decrease worker by", -delta, "for", strategy.Id, "on", s.scheduler.Id)
				discards := workers[len(workers)-utils.Abs(delta):]
				workers = workers[:len(workers)-utils.Abs(delta)]
				// stop them
				for _, w := range discards {
					w.Stop()
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
func (s *ScheduleManager) stopWorkers(strategyId string) {
	workers, ok := s.workersMap[strategyId]
	if false == ok {
		return
	}
	for _, w := range workers {
		w.Stop()
	}
	delete(s.workersMap, strategyId)
}

func (s *ScheduleManager) stopAllWorkers() {
	for k := range s.workersMap {
		s.stopWorkers(k)
	}
}

func (s *ScheduleManager) scheduleLoop() {
	// stop handler
	defer func() { s.shutdownNotifier <- 2 }()
	for !s.needStop {
		s.schedule()
		s.delay(s.scheduleInterval)
	}
	s.stopAllWorkers()
}
