package utils

import (
	"sync"

	"github.com/jasonjoo2010/goschedule/types"
)

type WorkerSet struct {
	mu      sync.Mutex
	workers map[string][]types.Worker
}

func NewWorkerSet() *WorkerSet {
	return &WorkerSet{
		workers: make(map[string][]types.Worker),
	}
}

func (set *WorkerSet) Strategies() []string {
	set.mu.Lock()
	defer set.mu.Unlock()

	if len(set.workers) == 0 {
		return nil
	}

	names := make([]string, 0, len(set.workers))
	for k := range set.workers {
		names = append(names, k)
	}

	return names
}

func (set *WorkerSet) WorkersFor(strategyName string) []types.Worker {
	set.mu.Lock()
	defer set.mu.Unlock()

	workers, ok := set.workers[strategyName]
	if !ok {
		return nil
	}

	result := make([]types.Worker, len(workers))
	copy(result, workers)
	return result
}

func (set *WorkerSet) Delete(strategyName string) {
	set.mu.Lock()
	defer set.mu.Unlock()

	delete(set.workers, strategyName)
}

func (set *WorkerSet) WorkersCountFor(strategyName string) int {
	set.mu.Lock()
	defer set.mu.Unlock()

	return len(set.workers[strategyName])
}

func (set *WorkerSet) AddWorker(strategyName string, w types.Worker) {
	set.mu.Lock()
	defer set.mu.Unlock()

	workers, ok := set.workers[strategyName]
	if !ok {
		workers = make([]types.Worker, 0, 1)
	}

	workers = append(workers, w)
	set.workers[strategyName] = workers
}

func (set *WorkerSet) RemoveWorker(strategyName string) types.Worker {
	set.mu.Lock()
	defer set.mu.Unlock()

	workers, ok := set.workers[strategyName]
	if !ok || len(workers) == 0 {
		return nil
	}

	w := workers[len(workers)-1]
	workers = workers[:len(workers)-1]
	if len(workers) == 0 {
		delete(set.workers, strategyName)
	} else {
		set.workers[strategyName] = workers
	}

	return w
}
