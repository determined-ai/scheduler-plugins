/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package coscheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Args defines the scheduling parameters for Coscheduling plugin.
type Args struct {
	// PermitWaitingTime is the wait timeout in seconds.
	PermitWaitingTimeSeconds int64
	// PodGroupGCInterval is the period to run gc of PodGroup in seconds.
	PodGroupGCIntervalSeconds int64
	// If the deleted PodGroup stays longer than the PodGroupExpirationTime,
	// the PodGroup will be deleted from PodGroupInfos.
	PodGroupExpirationTimeSeconds int64
}

// Coscheduling is a plugin that implements the mechanism of gang scheduling.
type Coscheduling struct {
	frameworkHandle framework.FrameworkHandle
	podLister       corelisters.PodLister
	// key is <namespace>/<PodGroup name> and value is *PodGroupInfo.
	podGroupInfos sync.Map
	// clock is used to get the current time.
	clock util.Clock
	// args is coscheduling parameters.
	args  Args
	gLock *sync.Mutex
	// approvedGroups is used to track what Pods are currently being scheduled.
	approvedGroups map[string]*waitingGroup
	// finishedgroups tracks what groups are done
	finishedGroups map[string]bool
	// lastRefresh controls when the scheduler rechecks for new pods
	lastRefresh time.Time
}

type waitingGroup struct {
	name         string
	minAvailable int
	slots        int
	priority     int32
	tolerations  []v1.Toleration
	selector     map[string]string
	position     decimal.Decimal
}

// PodGroupInfo is a wrapper to a PodGroup with additional information.
// A PodGroup's priority, timestamp and minAvailable are set according to
// the values of the PodGroup's first pod that is added to the scheduling queue.
type PodGroupInfo struct {
	// key is a unique PodGroup ID and currently implemented as <namespace>/<PodGroup name>.
	key string
	// name is the PodGroup name and defined through a Pod label.
	// The PodGroup name of a regular pod is empty.
	name string
	// priority is the priority of pods in a PodGroup.
	// All pods in a PodGroup should have the same priority.
	priority int32
	// timestamp stores the initialization timestamp of a PodGroup.
	timestamp time.Time
	// minAvailable is the minimum number of pods to be co-scheduled in a PodGroup.
	// All pods in a PodGroup should have the same minAvailable.
	minAvailable int
	// deletionTimestamp stores the timestamp when the PodGroup marked as expired.
	deletionTimestamp *time.Time
	// position stores the spot in the queue of the pod relative to others with the same priority
	position decimal.Decimal
}

// pathStringValue is a struct for the json payload passed to the k8s Patch function for pods.
// Patch allows us to modify some of the Pod's metadata, like Labels and Annotations.
type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

var _ framework.QueueSortPlugin = &Coscheduling{}
var _ framework.PreFilterPlugin = &Coscheduling{}
var _ framework.PermitPlugin = &Coscheduling{}
var _ framework.UnreservePlugin = &Coscheduling{}
var _ framework.ScorePlugin = &Coscheduling{}

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name = "Coscheduling"
	// PodGroupName is the name of a pod group that defines a coscheduling pod group.
	PodGroupName = "pod-group.scheduling.sigs.k8s.io/name"
	// PodGroupMinAvailable specifies the minimum number of pods to be scheduled together in a pod group.
	PodGroupMinAvailable = "pod-group.scheduling.sigs.k8s.io/min-available"
	QPosition            = "determined-queue-position"
	GpuResource          = "nvidia.com/gpu"
	SystemPriority       = 1000000
)

// Name returns name of the plugin. It is used in logs, etc.
func (cs *Coscheduling) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(config *runtime.Unknown, handle framework.FrameworkHandle) (framework.Plugin, error) {
	args := Args{
		PermitWaitingTimeSeconds:      10,
		PodGroupGCIntervalSeconds:     30,
		PodGroupExpirationTimeSeconds: 600,
	}

	if err := framework.DecodeInto(config, &args); err != nil {
		return nil, err
	}

	podLister := handle.SharedInformerFactory().Core().V1().Pods().Lister()
	cs := &Coscheduling{frameworkHandle: handle,
		podLister:      podLister,
		clock:          util.RealClock{},
		args:           args,
		gLock:          &sync.Mutex{},
		approvedGroups: map[string]*waitingGroup{},
		finishedGroups: map[string]bool{},
		lastRefresh:    time.Now(),
	}
	podInformer := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	podInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return responsibleForPod(t)
				case cache.DeletedFinalStateUnknown:
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return responsibleForPod(pod)
					}
					return false
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				DeleteFunc: cs.markPodGroupAsExpired,
			},
		},
	)
	go wait.Until(cs.podGroupInfoGC, time.Duration(cs.args.PodGroupGCIntervalSeconds)*time.Second, nil)

	return cs, nil
}

// Less is used to sort pods in the scheduling queue.
// 1. Compare the priorities of Pods.
// 2. Compare the initialization timestamps of PodGroups/Pods.
// 3. Compare the keys of PodGroups/Pods, i.e., if two pods are tied at priority and creation time, the one without podGroup will go ahead of the one with podGroup.
func (cs *Coscheduling) Less(podInfo1, podInfo2 *framework.PodInfo) bool {
	pgInfo1, _ := cs.getOrCreatePodGroupInfo(podInfo1.Pod, podInfo1.InitialAttemptTimestamp)
	pgInfo2, _ := cs.getOrCreatePodGroupInfo(podInfo2.Pod, podInfo2.InitialAttemptTimestamp)

	return cs.comparePgInfo(pgInfo1, pgInfo2)
}

// getOrCreatePodGroupInfo returns the existing PodGroup in PodGroupInfos if present.
// Otherwise, it creates a PodGroup and returns the value, It stores
// the created PodGroup in PodGroupInfo if the pod defines a  PodGroup and its
// PodGroupMinAvailable is greater than one. It also returns the pod's
// PodGroupMinAvailable (0 if not specified).
func (cs *Coscheduling) getOrCreatePodGroupInfo(pod *v1.Pod, ts time.Time) (*PodGroupInfo, int) {
	podGroupName, podMinAvailable, qPosition, _ := GetPodGroupLabels(pod)

	var pgKey string
	if len(podGroupName) > 0 && podMinAvailable > 0 {
		pgKey = fmt.Sprintf("%v/%v", pod.Namespace, podGroupName)
	}

	// If it is a PodGroup and present in PodGroupInfos, return it.
	if len(pgKey) != 0 {
		value, exist := cs.podGroupInfos.Load(pgKey)
		if exist {
			pgInfo := value.(*PodGroupInfo)
			changed := false
			// If the deleteTimestamp isn't nil, it means that the PodGroup is marked as expired before.
			// So we need to set the deleteTimestamp as nil again to mark the PodGroup active.
			if pgInfo.deletionTimestamp != nil {
				pgInfo.deletionTimestamp = nil
				changed = true
			}
			if !pgInfo.position.Equal(qPosition) {
				pgInfo.position = qPosition
				changed = true
			}
			priority := podutil.GetPodPriority(pod)
			if pgInfo.priority != priority {
				pgInfo.priority = priority
				changed = true
			}
			if changed {
				cs.podGroupInfos.Store(pgKey, pgInfo)
			}
			return pgInfo, podMinAvailable
		}
	}

	// If the PodGroup is not present in PodGroupInfos or the pod is a regular pod,
	// create a PodGroup for the Pod and store it in PodGroupInfos if it's not a regular pod.
	pgInfo := &PodGroupInfo{
		name:         podGroupName,
		key:          pgKey,
		priority:     podutil.GetPodPriority(pod),
		timestamp:    ts,
		minAvailable: podMinAvailable,
		position:     qPosition,
	}

	// If it's not a regular Pod, store the PodGroup in PodGroupInfos
	if len(pgKey) > 0 {
		cs.podGroupInfos.Store(pgKey, pgInfo)
	}
	return pgInfo, podMinAvailable
}

// PreFilter performs the following validations.
// 1. Validate if the PodGroup still exists and the pods are still pending
// 2. If there isn't enough space in the cluster, tag lesser priority Pods for preemption
// 3. Unless approved, Pods are marked as UnschedulableAndUnresolvable and put back in the scheduling queue
func (cs *Coscheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) *framework.Status {
	pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
	pgKey := pgInfo.key
	if len(pgKey) == 0 {
		return framework.NewStatus(framework.Success, "")
	}

	if time.Since(cs.lastRefresh).Seconds() > 5 {
		// check the groups to see if they have pods alive
		for group := range cs.approvedGroups {
			if cs.calculateTotalPods(group, "default") == 0 {
				delete(cs.approvedGroups, group)
			}
		}
		cs.lastRefresh = time.Now()
	}

	cs.gLock.Lock()
	if len(cs.approvedGroups) == 0 {
		cs.approvedGroups = map[string]*waitingGroup{}
		cs.getNewWaitingGroups()
		cs.lastRefresh = time.Now()
	}
	cs.gLock.Unlock()

	_, ok := cs.approvedGroups[pgInfo.name]
	if !ok {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"Pod is too low priority to be scheduled or backfilled")
	}

	return framework.NewStatus(framework.Success, "")
}

// PreFilterExtensions returns nil.
func (cs *Coscheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (cs *Coscheduling) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (cs *Coscheduling) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	if cs.calculateSlotRequest(p, true) == 1 {
		node, err := cs.frameworkHandle.SnapshotSharedLister().NodeInfos().Get(nodeName)
		if err == nil {
			slotsAvailable := cs.getSlotsAvailable(node, true)
			if slotsAvailable > 0 && slotsAvailable != cs.getMaxSlots(node, true) {
				return 100, framework.NewStatus(framework.Success)
			}
		}
	} else {
		node, err := cs.frameworkHandle.SnapshotSharedLister().NodeInfos().Get(nodeName)
		if err == nil {
			slotsAvailable := cs.getSlotsAvailable(node, true)
			if slotsAvailable > 0 && slotsAvailable == cs.getMaxSlots(node, true) {
				return 100, framework.NewStatus(framework.Success)
			}
		}
	}
	return 0, framework.NewStatus(framework.Success)
}

// Permit is the functions invoked by the framework at "Permit" extension point.
func (cs *Coscheduling) Permit(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
	if len(pgInfo.key) == 0 {
		return framework.NewStatus(framework.Success, ""), 0
	}

	cs.gLock.Lock()
	group, ok := cs.approvedGroups[pgInfo.name]
	if !ok {
		cs.gLock.Unlock()
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"Pod is not permitted because it was not approved"), 0
	}
	group.minAvailable -= 1
	if group.minAvailable == 0 {
		delete(cs.approvedGroups, group.name)
		cs.finishedGroups[group.name] = true
		cs.pruneFinishedGroups()
	} else {
		cs.approvedGroups[group.name] = group
	}
	cs.gLock.Unlock()

	return framework.NewStatus(framework.Success, ""), 0
}

// Unreserve rejects all other Pods in the PodGroup when one of the pods in the group times out.
func (cs *Coscheduling) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
	if len(pgInfo.key) == 0 {
		return
	}
	podGroupName := pgInfo.name
	cs.frameworkHandle.IterateOverWaitingPods(func(waitingPod framework.WaitingPod) {
		if waitingPod.GetPod().Namespace == pod.Namespace && waitingPod.GetPod().Labels[PodGroupName] == podGroupName {
			klog.V(3).Infof("Unreserve rejects the pod: %v/%v", podGroupName, waitingPod.GetPod().Name)
			waitingPod.Reject(cs.Name())
		}
	})
}

// GetPodGroupLabels checks if the pod belongs to a PodGroup. If so, it will return the
// podGroupName, minAvailable, queue position of the PodGroup. If not, it will return "", 0, and 0.
func GetPodGroupLabels(pod *v1.Pod) (string, int, decimal.Decimal, error) {
	podGroupName, exist := pod.Labels[PodGroupName]
	if !exist || len(podGroupName) == 0 {
		return "", 0, decimal.Zero, nil
	}
	minAvailable, exist := pod.Labels[PodGroupMinAvailable]
	if !exist || len(minAvailable) == 0 {
		return "", 0, decimal.Zero, nil
	}
	minNum, err := strconv.Atoi(minAvailable)
	if err != nil {
		klog.Errorf("PodGroup %v/%v : PodGroupMinAvailable %v is invalid", pod.Namespace, pod.Name, minAvailable)
		return "", 0, decimal.Zero, err
	}
	if minNum < 1 {
		klog.Errorf("PodGroup %v/%v : PodGroupMinAvailable %v is less than 1", pod.Namespace, pod.Name, minAvailable)
		return "", 0, decimal.Zero, err
	}
	qLabel, exist := pod.Labels[QPosition]
	if !exist {
		return podGroupName, minNum, decimal.Zero, nil
	}
	qPosition, err := decimal.NewFromString(qLabel)
	if err != nil {
		klog.Errorf("PodGroup %v/%v : %s", pod.Namespace, pod.Name, err)
		return podGroupName, minNum, decimal.Zero, err
	}
	return podGroupName, minNum, qPosition, nil
}

func (cs *Coscheduling) calculateTotalPods(podGroupName, namespace string) int {
	// TODO get the total pods from the scheduler cache and queue instead of the hack manner.
	selector := labels.Set{PodGroupName: podGroupName}.AsSelector()
	pods, err := cs.podLister.Pods(namespace).List(selector)
	if err != nil {
		klog.Error(err)
		return 0
	}
	return len(pods)
}

// markPodGroupAsExpired set the deletionTimestamp of PodGroup to mark PodGroup as expired.
func (cs *Coscheduling) markPodGroupAsExpired(obj interface{}) {
	pod := obj.(*v1.Pod)
	podGroupName, podMinAvailable, _, _ := GetPodGroupLabels(pod)
	if len(podGroupName) == 0 || podMinAvailable == 0 {
		return
	}

	pgKey := fmt.Sprintf("%v/%v", pod.Namespace, podGroupName)
	// If it's a PodGroup and present in PodGroupInfos, set its deletionTimestamp.
	value, exist := cs.podGroupInfos.Load(pgKey)
	if !exist {
		return
	}
	pgInfo := value.(*PodGroupInfo)
	if pgInfo.deletionTimestamp == nil {
		now := cs.clock.Now()
		pgInfo.deletionTimestamp = &now
		cs.podGroupInfos.Store(pgKey, pgInfo)
	}
}

// responsibleForPod selects pod that belongs to a PodGroup.
func responsibleForPod(pod *v1.Pod) bool {
	podGroupName, podMinAvailable, _, _ := GetPodGroupLabels(pod)
	if len(podGroupName) == 0 || podMinAvailable == 0 {
		return false
	}
	return true
}

func (cs *Coscheduling) podGroupInfoGC() {
	cs.podGroupInfos.Range(func(key, value interface{}) bool {
		pgInfo := value.(*PodGroupInfo)
		if pgInfo.deletionTimestamp != nil && pgInfo.deletionTimestamp.Add(time.Duration(cs.args.PodGroupExpirationTimeSeconds)*time.Second).Before(cs.clock.Now()) {
			klog.V(3).Infof("%v is out of date and has been deleted in PodGroup GC", key)
			cs.podGroupInfos.Delete(key)
		}
		return true
	})
}

// getNewWaitingGroups calculates what nodes should be scheduled given what pods are pending and currently scheduled.
// Backfilling is enabled by default.
// It updates the approvedGroups map with all the pods that can fit in the cluster.
func (cs *Coscheduling) getNewWaitingGroups() {
	hpGroups := map[string]*waitingGroup{} // contains the highest priority groups
	encounteredGroups := map[string]*waitingGroup{}
	var groupsList []*waitingGroup

	podsList := cs.getWaitingPods("default") // sorted by priority
	if podsList == nil || len(podsList.Items) == 0 {
		return
	}
	// translate all pods into groups
	for _, p := range podsList.Items {
		pgInfo, _ := cs.getOrCreatePodGroupInfo(&p, p.CreationTimestamp.Time)
		nextGroup := &waitingGroup{
			name:         pgInfo.name,
			minAvailable: pgInfo.minAvailable,
			priority:     *p.Spec.Priority,
			tolerations:  p.Spec.Tolerations,
			selector:     p.Spec.NodeSelector,
			slots:        cs.calculateSlotRequest(&p, true),
			position:     pgInfo.position,
		}
		if _, ok := encounteredGroups[pgInfo.name]; !ok {
			groupsList = append(groupsList, nextGroup)
		}

		encounteredGroups[pgInfo.name] = nextGroup
	}

	relatedGroups := map[string]string{}
	skip := false
	// process groups and keep a list of the highest priority ones per "resource pool"
	for _, pg := range groupsList {
		if _, ok := cs.finishedGroups[pg.name]; ok {
			continue
		}
		skip = false
		if len(hpGroups) > 0 {
			for parentGroup, waitGroup := range hpGroups {
				if cs.compareSelectors(pg.selector, waitGroup.selector) {
					relatedGroups[pg.name] = parentGroup
					skip = true
					break
				}
			}
		}

		if skip {
			continue
		}

		hpGroups[pg.name] = pg
	}

	cs.checkFits(groupsList, relatedGroups, hpGroups)
}

// checkFits is a function calculates how many of the pods fit the given resources
func (cs *Coscheduling) checkFits(groupsList []*waitingGroup, relatedGroups map[string]string,
	hpGroups map[string]*waitingGroup) {
	availableNodes := map[string]int{}
	extraSlots := map[string]int{}
	maxSlotsMap := map[string]int{}

	// first process all the priority groups
	for _, group := range hpGroups {
		numAvailable, freeSlots, maxSlots := cs.calculateAvailableNodes(group)

		if group.slots == 1 && freeSlots >= 1 { // if single slot, use free slots
			cs.approvedGroups[group.name] = group
			freeSlots -= 1
		} else if numAvailable >= group.minAvailable { // if not enough slots or larger exp, check if minavailable met
			cs.approvedGroups[group.name] = group
			numAvailable -= group.minAvailable
			if group.slots < maxSlots {
				freeSlots += maxSlots - group.slots
			}
		} else { // else preemption
			ok, nodesFreed := cs.preemptPods(group, numAvailable)
			if ok {
				cs.approvedGroups[group.name] = group
				numAvailable = numAvailable - group.minAvailable + nodesFreed
			}
		}
		availableNodes[group.name] = numAvailable
		extraSlots[group.name] = freeSlots
		maxSlotsMap[group.name] = maxSlots
	}

	// backfill action
	for _, group := range groupsList {
		if _, ok := hpGroups[group.name]; ok {
			continue
		}
		hpParent, _ := relatedGroups[group.name]
		if group.minAvailable == 1 && group.slots == 1 {
			if i, ok := extraSlots[hpParent]; ok && i >= 1 {
				cs.approvedGroups[group.name] = group
				extraSlots[hpParent] -= 1
			} else if availableNodes[hpParent] >= 1 {
				availableNodes[hpParent] -= 1
				extraSlots[hpParent] += maxSlotsMap[hpParent] - 1
				cs.approvedGroups[group.name] = group
			}
		} else if group.minAvailable == 1 && availableNodes[hpParent] > 1 {
			availableNodes[hpParent] -= 1
			maxSlots := maxSlotsMap[hpParent]
			if group.slots < maxSlots {
				extraSlots[hpParent] += maxSlots - group.slots
			}
		} else {
			if group.minAvailable <= availableNodes[hpParent] {
				cs.approvedGroups[group.name] = group
				availableNodes[hpParent] -= group.minAvailable
			}
		}
	}
}

// getWaitingPods returns a sorted list of pods that have not been scheduled yet
func (cs *Coscheduling) getWaitingPods(namespace string) *v1.PodList {
	fieldSelector := fmt.Sprintf("%s,%s", "status.phase=Pending",
		fields.SelectorFromSet(fields.Set{"spec.nodeName": ""}).String())

	podsList, err := cs.frameworkHandle.ClientSet().CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "determined",
		FieldSelector: fieldSelector,
	})

	if err != nil {
		klog.Error(err)
		return nil
	}

	sort.Slice(podsList.Items, func(i, j int) bool {
		pgInfo1, _ := cs.getOrCreatePodGroupInfo(&podsList.Items[i], podsList.Items[i].CreationTimestamp.Time)
		pgInfo2, _ := cs.getOrCreatePodGroupInfo(&podsList.Items[j], podsList.Items[j].CreationTimestamp.Time)

		return cs.comparePgInfo(pgInfo1, pgInfo2)
	})

	return podsList
}

func (cs *Coscheduling) comparePgInfo(pgInfo1, pgInfo2 *PodGroupInfo) bool {
	priority1 := pgInfo1.priority
	priority2 := pgInfo2.priority

	if priority1 != priority2 {
		return priority1 > priority2
	}

	position1 := pgInfo1.position
	position2 := pgInfo2.position
	if !position1.Equal(position2) {
		if !position1.Equal(decimal.Zero) && !position2.Equal(decimal.Zero) {
			return position1.LessThan(position2)
		} else {
			return position1.GreaterThan(position2) //if one of them is zero, treat the zero as if it's at the end of the queue
		}
	}

	time1 := pgInfo1.timestamp
	time2 := pgInfo2.timestamp

	if !time1.Equal(time2) {
		return time1.Before(time2)
	}

	return pgInfo1.key < pgInfo2.key
}

func (cs *Coscheduling) getAllNodes() []*schedulernodeinfo.NodeInfo {
	nodes, err := cs.frameworkHandle.SnapshotSharedLister().NodeInfos().List()

	if err != nil {
		klog.Error(err)
		return nil
	}
	return nodes
}

func (cs *Coscheduling) fitSelector(selector map[string]string, node *v1.Node) bool {
	for k, v := range selector {
		v2, ok := node.Labels[k]
		if !ok || v != v2 {
			return false
		}
	}
	return true
}

func (cs *Coscheduling) compareSelectors(s1, s2 map[string]string) bool {
	if s1 == nil && s2 == nil {
		return true
	}
	if s1 == nil || s2 == nil {
		return false
	}

	if len(s1) != len(s2) {
		return false
	}

	for k, v := range s1 {
		v2, ok := s2[k]
		if !ok {
			return false
		}
		if v != v2 {
			return false
		}
	}
	return true
}

func (cs *Coscheduling) doesTolerate(tolerations []v1.Toleration, taints []v1.Taint) bool {
	tolerationMap := map[int]v1.Toleration{}
	for i, toleration := range tolerations {
		tolerationMap[i] = toleration
	}

	for _, taint := range taints {
		found := -1
		if len(tolerationMap) == 0 {
			return false
		}
		for k, t := range tolerationMap {
			if t.ToleratesTaint(&taint) {
				found = k
				break
			}
		}
		if found < 0 { // if the taint can't find a matching toleration
			return false
		}
		delete(tolerationMap, found)
	}
	return true
}

func (cs *Coscheduling) preemptionTag(pod *v1.Pod) {
	if _, ok := pod.Labels["determined-preemption"]; ok {
		return
	}

	pod.Labels["determined-preemption"] = "true"
	payload := []patchStringValue{{
		Op:    "replace",
		Path:  "/metadata/labels/determined-preemption",
		Value: "yes",
	}}

	payloadBytes, _ := json.Marshal(payload)

	_, err := cs.frameworkHandle.ClientSet().CoreV1().Pods("default").Patch(
		context.TODO(), pod.Name, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	if err == nil {
		klog.V(3).Infof("Tagged pod %v for preemption", pod.Name)
	} else {
		klog.V(3).Infof("Unable to tag pod %v for preemption", pod.Name)
		klog.V(3).Infof("%v", err)
	}
}

// preemptPods returns a boolean that indicates whether or not preemption succeeded
// as well as an integer indicating how many nodes it has vacated
func (cs *Coscheduling) preemptPods(group *waitingGroup, available int) (bool, int) {
	klog.V(3).Infof("Preemption required! Finding preemption candidates")

	nodes := cs.getAllNodes()
	if nodes == nil {
		klog.V(3).Infof("There are no nodes ready.")
		return false, 0
	}

	//selectedNodes contains the nodes that fit the group's selector and taints
	selectedNodes := map[string]*schedulernodeinfo.NodeInfo{}
	for _, node := range nodes {
		if cs.fitSelector(group.selector, node.Node()) {
			taint, err := node.Taints()
			if err != nil {
				continue
			}
			if cs.doesTolerate(group.tolerations, taint) && cs.getUsedSlots(node, true) > 0 {
				selectedNodes[node.Node().Name] = node
			}
		}
	}

	needed := group.minAvailable - available

	if len(selectedNodes) < needed {
		klog.V(3).Infof("No preemption occurred. Needed %d nodes, but only %d available.", needed, len(selectedNodes))
		return false, 0
	}

	// for each node, find the highest priority pod and use that for preemption metric
	// priorities contains the highest priority pod for each node
	priorities := map[string]int{}
	queueOrders := map[string]decimal.Decimal{}
	timestamps := map[string]time.Time{}
	for _, node := range nodes {
		maximum := 0
		bestPos := decimal.Zero
		ts := time.Now()
		if cs.getUsedSlots(node, true) == 0 {
			continue
		}
		for _, pod := range node.Pods() {
			pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
			if _, ok := pod.Labels["determined-system"]; ok {
				maximum = SystemPriority
				bestPos = decimal.Zero
			} else if _, ok := pod.Labels["determined-cmd"]; ok {
				maximum = SystemPriority
				bestPos = decimal.Zero
			} else if _, ok := pod.Labels["determined"]; !ok { //only count determined pods for preemption
				continue
			}
			if int(pgInfo.priority) > maximum {
				maximum = int(*pod.Spec.Priority)
				bestPos = pgInfo.position
				ts = pgInfo.timestamp
			} else if int(pgInfo.priority) == maximum && pgInfo.timestamp.Before(ts) {
				ts = pgInfo.timestamp
			}
		}
		priorities[node.Node().Name] = maximum
		queueOrders[node.Node().Name] = bestPos
		timestamps[node.Node().Name] = ts
	}

	// the lowest priority and most recent node gets preempted first
	var nodesToPreempt []string
	for needed > 0 {
		minQPos := decimal.Zero
		minKey := ""
		minimum := math.MaxInt32
		ts := time.Now()
		for k, v := range priorities {
			if minQPos.Equal(decimal.Zero) {
				minQPos = queueOrders[k]
			}
			if v < minimum {
				minimum = v
				minKey = k
				ts, _ = timestamps[k]
			} else if v == minimum && queueOrders[k].LessThan(minQPos) {
				minKey = k
				minQPos = queueOrders[k]
				ts, _ = timestamps[k]
			} else if v == minimum && queueOrders[k] == minQPos && timestamps[k].After(ts) {
				minKey = k
				ts, _ = timestamps[k]
			}
		}
		if minimum > int(group.priority) {
			break
		} else if minimum == int(group.priority) && minQPos.LessThanOrEqual(group.position){
			break
		}
		nodesToPreempt = append(nodesToPreempt, minKey)
		delete(priorities, minKey)
		needed -= 1
	}

	if needed > 0 {
		klog.V(3).Infof("No preemption occurred. Not enough nodes are able to be freed")
		return false, 0
	}

	// start preemption for nodes in the list
	preemptionGroups := map[string]bool{}
	for _, nodeName := range nodesToPreempt {
		klog.V(3).Infof("Preempting pods on node %v", nodeName)
		node, _ := selectedNodes[nodeName]
		for _, pod := range node.Pods() {
			if _, ok := pod.Labels["determined"]; ok { //only preempt determined pods
				pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
				preemptionGroups[pgInfo.name] = true
				cs.preemptionTag(pod)
			}
		}
	}

	// priorities now contains the nodes that have not been selected for preemption yet
	// Check to make sure that for every pod that is preempted, we also preempt their entire pod group
	additionalPreemptions := 0
	for name, _ := range priorities {
		node, _ := selectedNodes[name]
		shouldPreempt := false
		for _, pod := range node.Pods() {
			pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
			if _, ok := preemptionGroups[pgInfo.name]; ok {
				shouldPreempt = true
				break
			}
		}
		if shouldPreempt {
			klog.V(3).Infof("Preempting pods on node %v", node.Node().Name)
			for _, pod := range node.Pods() {
				if _, ok := pod.Labels["determined"]; ok {
					cs.preemptionTag(pod)
				}
			}
			additionalPreemptions += 1
		}
	}

	return true, group.minAvailable - available + additionalPreemptions
}

// calculateAvailableNodes calculates the number of nodes available for scheduling
// It returns three ints:
// the number of nodes available now that fit the pod's selector and tolerances,
// the number of slots available on partially filled nodes
// and the maxSlots available on the nodes
func (cs *Coscheduling) calculateAvailableNodes(podgroup *waitingGroup) (int, int, int) {
	klog.V(9).Infof("Finding fits for podgroup %v\n", podgroup.name)

	nodesAvailable := 0
	freeSlots := 0
	returnedMaxSlots := 0

	nodes := cs.getAllNodes()
	if nodes == nil {
		return 0, 0, 0
	}

	for _, node := range nodes {
		if !cs.fitSelector(podgroup.selector, node.Node()) {
			klog.V(9).Infof("NODE %v doesn't have the right label\n", node.Node().Name)
			continue
		}

		taints, err := node.Taints()
		if err != nil {
			continue
		}

		if !cs.doesTolerate(podgroup.tolerations, taints) {
			klog.V(9).Infof("NODE %v doesn't fit\n", node.Node().Name)
			continue
		}
		// if node passes the selector and tolerances, check if there are slots available
		slotsAvailable := cs.getSlotsAvailable(node, true)
		maxSlots := cs.getMaxSlots(node, true)
		if maxSlots > returnedMaxSlots {
			returnedMaxSlots = maxSlots
		}

		if slotsAvailable > 0 && slotsAvailable == maxSlots {
			// the node is fully available
			nodesAvailable += 1
		} else {
			freeSlots += slotsAvailable
		}
	}

	return nodesAvailable, freeSlots, returnedMaxSlots
}

// calculateSlotRequest returns the slots requested by the pod
func (cs *Coscheduling) calculateSlotRequest(pod *v1.Pod, gpu bool) int {
	slotsNeeded := 0
	for _, c := range pod.Spec.Containers {
		if gpu {
			slots, ok := c.Resources.Requests[GpuResource]
			if !ok {
				continue
			}
			slotsNeeded += int(slots.Value())
		} else {
			slotsNeeded += int(c.Resources.Limits.Cpu().Value())
		}
	}

	return slotsNeeded
}

func (cs *Coscheduling) getUsedSlots(n *schedulernodeinfo.NodeInfo, gpu bool) int {
	usedSlots := 0
	for _, pod := range n.Pods() {
		usedSlots += cs.calculateSlotRequest(pod, gpu)
	}
	return usedSlots
}

func (cs *Coscheduling) getSlotsAvailable(n *schedulernodeinfo.NodeInfo, gpu bool) int {
	total := cs.getMaxSlots(n, gpu)
	if total == 0 {
		return 0
	}
	return total - cs.getUsedSlots(n, gpu)
}

func (cs *Coscheduling) getMaxSlots(n *schedulernodeinfo.NodeInfo, gpu bool) int {
	if gpu {
		slots, ok := n.Node().Status.Allocatable[GpuResource]
		if !ok {
			return 0
		}
		return int(slots.Value())
	}
	return int(n.Node().Status.Capacity.Cpu().Value())
}

func (cs *Coscheduling) pruneFinishedGroups() {
	newFinishedGroups := map[string]bool{}
	for _, pod := range cs.getWaitingPods("default").Items {
		if _, ok := cs.finishedGroups[pod.Name]; ok {
			newFinishedGroups[pod.Name] = true
		}
	}
	cs.finishedGroups = newFinishedGroups
}
