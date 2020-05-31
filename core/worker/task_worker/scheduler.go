package task_worker

import (
	"fmt"
	"sort"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
)

const (
	RUNTIME_EMPTY = "<empty>"
)

type runtimeAssign struct {
	RuntimeId string
	Items     []string
}

func (r *runtimeAssign) String() string {
	return fmt.Sprint("{", r.RuntimeId, ",", r.Items, "}")
}

func (w *TaskWorker) clearExpiredRuntimes() ([]string, []*definition.TaskRuntime, error) {
	now := time.Now().Unix() * 1000
	runtimes, err := w.store.GetTaskRuntimes(w.strategyDefine.Id, w.taskDefine.Id)
	if err != nil || len(runtimes) < 1 {
		return nil, nil, err
	}
	uuids := make([]string, 0, len(runtimes))
	validRuntimes := make([]*definition.TaskRuntime, 0, len(runtimes))
	for _, r := range runtimes {
		// expired?
		if now-r.LastHeartBeat > int64(w.taskDefine.DeathTimeout) {
			w.store.RemoveTaskRuntime(w.runtime.StrategyId, w.runtime.TaskId, r.Id)
			logrus.Warn("Clean expired task runtime: ", r.Id)
			continue
		}
		uuids = append(uuids, r.Id)
		validRuntimes = append(validRuntimes, r)
	}
	return uuids, validRuntimes, nil
}

func (w *TaskWorker) getCurrentAssignments() (map[string]*definition.TaskAssignment, []*definition.TaskAssignment, []*runtimeAssign, error) {
	assignments, err1 := w.store.GetTaskAssignments(w.strategyDefine.Id, w.taskDefine.Id)
	shouldReload := false
	// clear dirty task items first
	for _, assign := range assignments {
		if !utils.ContainsTaskItem(w.taskDefine.Items, assign.ItemId) {
			w.store.RemoveTaskAssignment(w.strategyDefine.Id, w.taskDefine.Id, assign.ItemId)
			logrus.Warn("Clear undefined task item: ", assign.ItemId)
			shouldReload = true
		}
	}
	if shouldReload {
		assignments, _ = w.store.GetTaskAssignments(w.strategyDefine.Id, w.taskDefine.Id)
	}
	runtimes, err2 := w.store.GetTaskRuntimes(w.strategyDefine.Id, w.taskDefine.Id)
	if err1 != nil || err2 != nil {
		if err1 != nil {
			logrus.Error("Fetch assignments of task error: ", err1.Error())
			return nil, nil, nil, err1
		}
		if err2 != nil {
			logrus.Error("Fetch runtimes of task error: ", err2.Error())
			return nil, nil, nil, err2
		}
	}
	assignMap := make(map[string]*definition.TaskAssignment)
	runtimesMap := make(map[string]*runtimeAssign)
	for _, r := range runtimes {
		runtimesMap[r.Id] = &runtimeAssign{
			RuntimeId: r.Id,
			Items:     make([]string, 0, 1),
		}
	}
	// Make sure all items having assignment info
	spareAssignments := make([]*definition.TaskAssignment, 0, 1)
	for _, t := range assignments {
		assignMap[t.ItemId] = t
		rid := t.RequestedRuntimeId
		if rid == "" {
			rid = t.RuntimeId
		}
		if rid == RUNTIME_EMPTY {
			continue
		}
		if rid == "" {
			spareAssignments = append(spareAssignments, t)
			continue
		}
		if r, ok := runtimesMap[rid]; !ok {
			// abnormal
			logrus.Warn("Specific runtime of assignment cannot be found: ", rid)
			t.RuntimeId = ""
			t.RequestedRuntimeId = ""
			spareAssignments = append(spareAssignments, t)
		} else {
			r.Items = append(r.Items, t.ItemId)
		}
	}
	for _, t := range w.taskDefine.Items {
		var (
			assignRemote *definition.TaskAssignment
			ok           bool
		)
		if assignRemote, ok = assignMap[t.Id]; !ok {
			// not exist
			assign := &definition.TaskAssignment{
				StrategyId: w.strategyDefine.Id,
				TaskId:     w.taskDefine.Id,
				ItemId:     t.Id,
				Parameter:  t.Parameter,
			}
			spareAssignments = append(spareAssignments, assign)
			w.store.SetTaskAssignment(assign)
			assignMap[t.Id] = assign
			continue
		}
		// check consistent
		if assignRemote.Parameter != t.Parameter {
			assignRemote.Parameter = t.Parameter
			w.store.SetTaskAssignment(assignRemote)
			continue
		}
	}
	runtimeAssigns := make([]*runtimeAssign, 0, len(runtimesMap))
	for _, r := range runtimesMap {
		runtimeAssigns = append(runtimeAssigns, r)
	}
	sort.Slice(runtimeAssigns, func(i, j int) bool {
		return len(runtimeAssigns[i].Items) > len(runtimeAssigns[j].Items)
	})
	return assignMap, spareAssignments, runtimeAssigns, nil
}

func (w *TaskWorker) distributeTaskItems() {
	uuids, validRuntimes, err := w.clearExpiredRuntimes()
	if err != nil {
		logrus.Error("Fetch runtimes of task failed: ", err.Error())
		return
	}
	if len(validRuntimes) < 1 {
		// ignore
		return
	}
	// is leader?
	if !utils.IsLeader(uuids, w.runtime.Id) {
		return
	}
	assignMap, spares, assigned, err := w.getCurrentAssignments()
	if err != nil {
		logrus.Error("Fetch assignments of task items error: ", err.Error())
	}
	// regenerate uuids array to guarantee consistence
	uuids = uuids[:0]
	for _, assign := range assigned {
		uuids = append(uuids, assign.RuntimeId)
	}
	if len(uuids) < 1 {
		// empty runtimes
		return
	}
	// try balance the task items
	items := w.taskDefine.Items
	balanced := utils.AssignWorkers(len(uuids), len(items), w.taskDefine.MaxTaskItems)
	var changed bool
	for pos, target := range balanced {
		cur := assigned[pos]
		cnt := len(cur.Items)
		if cnt == target {
			continue
		}
		changed = true
		if cnt > target {
			// decrease
			for i := 0; i < cnt-target; i++ {
				len := len(cur.Items)
				itemId := cur.Items[len-1]
				cur.Items = cur.Items[:len-1]
				item := assignMap[itemId]
				item.RequestedRuntimeId = RUNTIME_EMPTY
				spares = append(spares, item)
				w.store.SetTaskAssignment(item)
			}
			logrus.Info("Decrease ", cnt-target, " task item(s) from ", cur.RuntimeId)
		} else if cnt < target {
			// increase
			for i := 0; i < target-cnt; i++ {
				len := len(spares)
				if len < 1 {
					logrus.Error("Not enough spared task item to assign")
					break
				}
				item := spares[len-1]
				cur.Items = append(cur.Items, item.ItemId)
				spares = spares[:len-1]
				if item.RuntimeId == "" {
					item.RuntimeId = cur.RuntimeId
					item.RequestedRuntimeId = ""
				} else {
					item.RequestedRuntimeId = cur.RuntimeId
				}
				w.store.SetTaskAssignment(item)
			}
			logrus.Info("Increase ", target-cnt, " task item(s) to ", cur.RuntimeId)
		}
	}
	if changed {
		w.store.IncreaseTaskItemsConfigVersion(w.strategyDefine.Id, w.taskDefine.Id)
	}
}

// assignTaskItems reloads task items and release items others requests
//	When call this PLEASE make sure that you have NO queued data in channel
func (w *TaskWorker) reloadTaskItems() {
	assignments, err := w.store.GetTaskAssignments(w.strategyDefine.Id, w.taskDefine.Id)
	if err != nil {
		logrus.Error("Fetch assignments error: ", err.Error())
		return
	}
	newItems := 0
	removedItems := 0
	for _, assignment := range assignments {
		if assignment.RuntimeId == "" {
			if assignment.RequestedRuntimeId == w.runtime.Id {
				// mine, update
				if !utils.ContainsTaskItem(w.taskItems, assignment.ItemId) {
					// remove from local
					w.taskItems = append(w.taskItems, definition.TaskItem{
						Id:        assignment.ItemId,
						Parameter: assignment.Parameter,
					})
					newItems++
				}
				assignment.RuntimeId = w.runtime.Id
				assignment.RequestedRuntimeId = ""
				w.store.SetTaskAssignment(assignment)
			} else {
				// not mine, none of my business
				if utils.ContainsTaskItem(w.taskItems, assignment.ItemId) {
					// remove from local
					w.taskItems = utils.RemoveTaskItem(w.taskItems, assignment.ItemId)
					removedItems++
				}
			}
			continue
		} else if assignment.RuntimeId != w.runtime.Id {
			// not mine
			continue
		}
		// current mine
		if assignment.RequestedRuntimeId != "" {
			// should release it
			// update TaskWorker first
			w.taskItems = utils.RemoveTaskItem(w.taskItems, assignment.ItemId)
			if assignment.RequestedRuntimeId == RUNTIME_EMPTY {
				assignment.RuntimeId = ""
			} else {
				assignment.RuntimeId = assignment.RequestedRuntimeId
			}
			assignment.RequestedRuntimeId = ""
			w.store.SetTaskAssignment(assignment)
			w.store.IncreaseTaskItemsConfigVersion(w.strategyDefine.Id, w.taskDefine.Id)
			logrus.Info("Release task item [", assignment.ItemId, "] for ", assignment.TaskId, " to ", assignment.RuntimeId)
			removedItems++
			continue
		}
		if !utils.ContainsTaskItem(w.taskItems, assignment.ItemId) {
			// mine, new
			w.taskItems = append(w.taskItems, definition.TaskItem{
				Id:        assignment.ItemId,
				Parameter: assignment.Parameter,
			})
			newItems++
		}
	}
	if newItems+removedItems == 0 {
		logrus.Info("Reload task items, no change")
	} else {
		logrus.Info("Reload task items, ", newItems, " items added, ", removedItems, " items released")
	}
}

func (w *TaskWorker) schedule() {
	// stop handler
	defer func() { w.notifierC <- 4 }()
	for !w.needStop {
		w.distributeTaskItems()
		utils.Delay(w, 10*time.Second)
	}
	assignments, err := w.store.GetTaskAssignments(w.strategyDefine.Id, w.taskDefine.Id)
	if err != nil {
		logrus.Warn("Fetch assignments failed: ", err.Error())
		return
	}
	for _, assignment := range assignments {
		if assignment.RuntimeId == w.runtime.Id {
			assignment.RuntimeId = ""
			w.store.SetTaskAssignment(assignment)
		}
	}
}
