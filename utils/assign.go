package utils

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
)

// shuffleRange shuffle specific range of slice
//	from(inclusive) to(exclusive)
func shuffleRange(runtimes []*definition.StrategyRuntime, from, to int) {
	if from >= to || to-from < 2 {
		return
	}
	N := to - from
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(N, func(i, j int) {
		i += from
		j += from
		runtimes[i], runtimes[j] = runtimes[j], runtimes[i]
	})
}

// SortRuntimes sort the runtimes based on requestedNum in descending order
//	A shuffle will be made between same num for better ditribution of workers
func SortRuntimesWithShuffle(runtimes []*definition.StrategyRuntime) {
	if len(runtimes) <= 1 {
		return
	}
	sort.Slice(runtimes, func(i, j int) bool {
		return runtimes[i].RequestedNum > runtimes[j].RequestedNum
	})
	pos := 0
	pre := runtimes[0].RequestedNum
	for i := 1; i < len(runtimes); i++ {
		if pre != runtimes[i].RequestedNum {
			shuffleRange(runtimes, pos, i)
			pos = i
			pre = runtimes[i].RequestedNum
		}
	}
	shuffleRange(runtimes, pos, len(runtimes))
}

func compareWithSequence(s1, s2 string) bool {
	pos1 := strings.LastIndexByte(s1, '$')
	pos2 := strings.LastIndexByte(s1, '$')
	if pos1 < 1 || pos2 < 1 {
		return pos1 < pos2
	}
	seq1, _ := strconv.Atoi(s1[pos1+1:])
	seq2, _ := strconv.Atoi(s2[pos2+1:])
	return seq1 < seq2
}

func SortSchedulers(schedulers []*definition.Scheduler) {
	if len(schedulers) <= 1 {
		return
	}
	sort.Slice(schedulers, func(i, j int) bool {
		return compareWithSequence(schedulers[i].Id, schedulers[j].Id)
	})
}

// AssignWorkers assigns workers between nodes and limit maximum per node.
//	limit = 0 indicates no limit at all
func AssignWorkers(nodeCount, workerCount, limit int) []int {
	workers := make([]int, nodeCount)
	if limit > 0 && workerCount > nodeCount*limit {
		workerCount = nodeCount * limit
	}
	avg := workerCount / nodeCount
	other := workerCount % nodeCount
	for i := 0; i < nodeCount; i++ {
		workers[i] = avg
		if i < other {
			workers[i]++
		}
	}
	return workers
}

// CanSchedule returns whether current scheduler can join into the specified strategy (based on its iplist)
//	ipList is the range that can be scheduled on including hostnames and ip addresses
//	hostname indicates current node's hostname
//	ip indicates current node's ip address
func CanSchedule(ipList []string, hostname, ip string) bool {
	len := len(ipList)
	if len < 1 {
		return false
	}
	for i := 0; i < len; i++ {
		if ipList[i] == "127.0.0.1" || ipList[i] == "localhost" ||
			ipList[i] == hostname || ipList[i] == ip {
			return true
		}
	}
	return false
}
