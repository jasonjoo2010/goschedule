package task_worker

import (
	"fmt"
	"sort"
	"time"

	"github.com/jasonjoo2010/goschedule/core/definition"
	"github.com/jasonjoo2010/goschedule/utils"
	"github.com/sirupsen/logrus"
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
	runtimes, err := w.store.GetTaskRuntimes(w.taskDefine.Id)
	if err != nil || len(runtimes) < 1 {
		return nil, nil, err
	}
	uuids := make([]string, 0, len(runtimes))
	validRuntimes := make([]*definition.TaskRuntime, 0, len(runtimes))
	for _, r := range runtimes {
		// expired?
		if now-r.LastHeartBeat > int64(w.taskDefine.DeathTimeout) {
			w.store.RemoveTaskRuntime(w.runtime.TaskId, r.Id)
			logrus.Warn("Clean expired task runtime: ", r.Id)
			continue
		}
		uuids = append(uuids, r.Id)
		validRuntimes = append(validRuntimes, r)
	}
	return uuids, validRuntimes, nil
}

func (w *TaskWorker) getCurrentAssignments() (map[string]*definition.TaskAssignment, []*definition.TaskAssignment, []*runtimeAssign, error) {
	assignments, err1 := w.store.GetTaskAssignments(w.taskDefine.Id)
	runtimes, err2 := w.store.GetTaskRuntimes(w.taskDefine.Id)
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
		if t.RuntimeId == "" && t.RequestedRuntimeId == "" {
			spareAssignments = append(spareAssignments, t)
		}
		rid := t.RuntimeId
		if rid == "" && t.RequestedRuntimeId != "" {
			rid = t.RequestedRuntimeId
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
		if _, ok := assignMap[t.Id]; !ok {
			assign := &definition.TaskAssignment{
				TaskId:     w.taskDefine.Id,
				ItemId:     t.Id,
				Paramenter: t.Parameter,
			}
			spareAssignments = append(spareAssignments, assign)
			w.store.SetTaskAssignment(assign)
			assignMap[t.Id] = assign
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
	// try balance the task items
	items := w.taskDefine.Items
	balanced := utils.AssignWorkers(len(uuids), len(items), w.taskDefine.MaxTaskItems)
	changedRuntimes := make([]string, 0)
	for pos, target := range balanced {
		cur := assigned[pos]
		cnt := len(cur.Items)
		if cnt == target {
			continue
		}
		if cnt > target {
			// decrease
			for i := 0; i < cnt-target; i++ {
				len := len(cur.Items)
				itemId := cur.Items[len-1]
				cur.Items = cur.Items[:len-1]
				item := assignMap[itemId]
				item.RequestedRuntimeId = ""
				spares = append(spares, item)
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
					changedRuntimes = append(changedRuntimes, cur.RuntimeId)
				} else {
					item.RequestedRuntimeId = cur.RuntimeId
					changedRuntimes = append(changedRuntimes, item.RuntimeId)
				}
				w.store.SetTaskAssignment(item)
			}
			logrus.Info("Increase ", target-cnt, " task item(s) to ", cur.RuntimeId)
		}
	}
	for _, rid := range changedRuntimes {
		w.store.RequireTaskReloadItems(w.taskDefine.Id, rid)
	}
	// TODO verify whether there may be consistence problem between reload flags and actual data
}

// assignTaskItems reloads task items and release items others requests
//	When call this PLEASE make sure that you have NO queued data in channel
func (w *TaskWorker) reloadTaskItems() {
	assignments, err := w.store.GetTaskAssignments(w.taskDefine.Id)
	if err != nil {
		logrus.Error("Fetch assignments error: ", err.Error())
		return
	}
	newItems := 0
	removedItems := 0
	for _, assignment := range assignments {
		if assignment.RuntimeId != "" &&
			assignment.RuntimeId != w.runtime.Id {
			// not mine
			continue
		}
		if assignment.RuntimeId == w.runtime.Id &&
			assignment.RequestedRuntimeId != "" {
			// mine, but should release it
			// update TaskWorker first
			w.taskItems = utils.RemoveTaskItem(w.taskItems, assignment.ItemId)
			assignment.RuntimeId = assignment.RequestedRuntimeId
			assignment.RequestedRuntimeId = ""
			w.store.SetTaskAssignment(assignment)
			w.store.RequireTaskReloadItems(w.taskDefine.Id, assignment.RuntimeId)
			logrus.Info("Release task item [", assignment.ItemId, "] for ", assignment.TaskId)
			removedItems++
			continue
		}
		if !utils.ContainsTaskItem(w.taskItems, assignment.ItemId) {
			// mine, new
			w.taskItems = append(w.taskItems, definition.TaskItem{
				Id:        assignment.ItemId,
				Parameter: assignment.Paramenter,
			})
			newItems++
		}
	}
	w.store.ClearTaskReloadItems(w.taskDefine.Id, w.runtime.Id)
	logrus.Info("Reload task items, ", newItems, " items added, ", removedItems, " items released")
}

func (w *TaskWorker) schedule() {
	// stop handler
	defer func() { w.notifierC <- 4 }()
	for !w.needStop {
		w.distributeTaskItems()
		utils.Delay(w, 60*time.Millisecond)
	}
	assignments, err := w.store.GetTaskAssignments(w.taskDefine.Id)
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
