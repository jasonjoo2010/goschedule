package utils

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
