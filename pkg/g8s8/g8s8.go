package g8s8

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	// "sigs.k8s.io/scheduler-plugins/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/g8s8/tracker"
	"sigs.k8s.io/scheduler-plugins/pkg/g8s8/protos"
)

type G8S8 struct {
	handle framework.Handle

	// trackers maps node names to the Tracker they should use for scheduling.
	// Each VIPU server has one *Tracker, and multiple nodes may map to one Tracker
	trackers map[string]*tracker.Tracker

	// preqs maps pod.UID to a PartitionRequest.  A PartitionRequest is added in Reserve, then it
	// remains until either Unreserve, or when the pod is deleted from the cluster.
	preqs map[types.UID]*tracker.PartitionRequest
}

var _ framework.FilterPlugin = &G8S8{}
var _ framework.ReservePlugin = &G8S8{}
var _ framework.PreBindPlugin = &G8S8{}

// Name is the name of the plugin used in the Registry and configurations.
const Name = "G8S8"

func (g *G8S8) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	_ = obj
	// TODO: figure out how to get this to compile
	// args, ok := obj.(*config.G8S8Args)
	// if !ok {
	// 	return nil, fmt.Errorf("want args to be of type G8S8Args, got %T", obj)
	// }

	// HACK: hardcode some configuration
	conn, err := grpc.Dial(
		"localhost:8191",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		panic(err.Error())
	}
	vc := &tracker.RealVIPUClient{
		protos.NewVersionServiceClient(conn),
		protos.NewUserServiceClient(conn),
		"cl1",
	}

	// In our fake-vipu setup, we have one VIPU server with 4 IPUs.
	t := tracker.NewTracker(context.Background(), vc, 4)

	// Create a map of IPU-capable nodes in the cluster to trackers for their VIPU servers.
	// When running kubernetes in `kind` we only have one node.
	trackers := map[string]*tracker.Tracker{
		"kind-control-plane": t,
	}

	g := &G8S8{
		handle: handle,
		trackers: trackers,
		preqs: map[types.UID]*tracker.PartitionRequest{},
	}

	// track when pods are deleted, so we can clean up the partitions we create

	// no need for periodic re-updates
	clientset := kubernetes.NewForConfigOrDie(handle.KubeConfig())
	informerFactory := informers.NewSharedInformerFactory(clientset, 0)

	podInformer := informerFactory.Core().V1().Pods()
	podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				pod, ok := obj.(*v1.Pod)
				// we want to capture pods which transition from pending/running
				return ok && (pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodRunning)
			},
			Handler: cache.ResourceEventHandlerFuncs{
				DeleteFunc: func(obj interface{}){
					pod, ok := obj.(*v1.Pod)
					if !ok {
						return
					}
					preq, ok := g.preqs[pod.UID]
					if !ok {
						return
					}
					_ = preq.Cancel()
					delete(g.preqs, pod.UID)
				},
			},
		},
	)

	// TODO: is there a better way to shut this down?
	stop := make(chan struct{})
	informerFactory.Start(stop)

	// TODO: need to wait for initial sync, and sync VIPU server, then try to recover from failures

	return g, nil
}

func ipusRequested(pod *v1.Pod) (uint32, *framework.Status) {
	label, ok := pod.Labels["ipus.graphcore.com"]
	if !ok {
		return 0, nil
	}
	nipus, err := strconv.ParseUint(label, 10, 32)
	if err != nil {
		return 0, framework.NewStatus(
			framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("invalid ipus.graphcore.com label: %q", label),
		)
	}
	return uint32(nipus), nil
}

// Filter out nodes which cannot run a pod.
func (g *G8S8) Filter(
	ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo,
) *framework.Status {
	// The FilterPlugin.Filter docstring indicates that there might be preemption evaluation
	// happening here, so we might need to detect it (where nodeInfo suggests there are fewer pods
	// attached to the node than we know there to be) and we need to calculate what the output of
	// Filter() would be if those other pods got preempted.
	//
	// TODO(rb): figure out how preemption in our plugin would interact with the coscheduling
	// plugin that we also want to support, and if it would not cause problems, then it should be
	// reasonably simple to support:
	// - identifing pods missing from nodeInfo, and corresponding slotIDs
	// - making a copy of tracker.used
	// - call clearUsed() for each slotID
	// - check if findFit() succeeds

	nipus, status := ipusRequested(pod)
	if status != nil {
		return status
	}

	if nipus == 0 {
		return framework.NewStatus(framework.Success)
	}

	nodeName := nodeInfo.Node().Name

	tracker, ok := g.trackers[nodeName]
	if !ok {
		return framework.NewStatus(
			framework.Unschedulable,
			fmt.Sprintf("no ipus on node %q", nodeName),
		)
	}

	if tracker.WouldFit(nipus) {
		return framework.NewStatus(framework.Success)
	}

	return framework.NewStatus(
		framework.Unschedulable,
		fmt.Sprintf("unable to fit partition of %v IPUs", nipus),
	)
}

// Reserve creates a PartitionRequest, which effectively reserves a set of IPUs for this pod.
func (g *G8S8) Reserve(
	ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string,
) *framework.Status {
	nipus, status := ipusRequested(pod)
	if status != nil {
		return status
	}

	if nipus == 0 {
		return framework.NewStatus(framework.Success)
	}

	// start partition creation
	g.preqs[pod.UID] = g.trackers[nodeName].RequestPartition(nipus)
	return framework.NewStatus(framework.Success)
}

// Unreserve cancels the PartitionRequest we started for this pod.
func (g *G8S8) Unreserve(
	ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string,
) {
	// cancel the partition we requested for this pod
	preq, ok := g.preqs[pod.UID]
	if ok {
		preq.Cancel()
		delete(g.preqs, pod.UID)
	}
}

// PreBind waits for the PartitionRequest to complete successfully.
func (g *G8S8) PreBind(
	ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string,
) *framework.Status {
	preq, ok := g.preqs[pod.UID]
	if ok {
		_, err := preq.Await(ctx)
		if err != nil {
			return framework.NewStatus(
				framework.Unschedulable,
				fmt.Sprintf("partition creation failed: %v", err),
			)
		}
	}
	return framework.NewStatus(framework.Success)
}
