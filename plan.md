# Graphcore Scheduler ("g8s8")

The graphcore scheduler ("g8s8") will be developed as a plugin to the
[Scheduling Framework API](
https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework
) of the default k8s scheduler.

## Why is a custom scheduler needed?

Nvidia and AMD don't need custom schedulers, why does Graphcore hardware
require a custom scheduler?

The answer is that k8s only knows how to model hardware-attached resources.  If
a pod requests 10GB of RAM, the node where that pod runs has 10GB of RAM less
to offer other pods, but other nodes in the cluster are unaffected.

However, Graphcore hardware is network-attached.  If a k8s pod requests 16
IPUs, then that node has 16 fewer IPUs to offer, but so do all other k8s nodes
which share the same IPU POD system.

In k8s, it is the scheduler that is responsible for tracking resource
allocations, and not any other part of the k8s cluster.  This ensures that the
source of truth for allocated resources is in the same process as the
decision-making about allocating new resources, and avoids a lot of complex
distributed locking scenarios which could otherwise arise.

But since the k8s scheduler tracks resources, and doesn't know how to track
network-attached resources, Graphcore needs a custom scheduler capable of
tracking IPU allocations.

## Why write a plugin instead of a from-scratch scheduler?

The primary motivation for extending the default scheduler is that the k8s
scheduler is responsible for many of the things that k8s users expect from the
k8s cluster: resource tracking, taints and tolerations, node affinity, etc.  A
from-scratch k8s scheduler would need to re-implement all of this tracking
logic.

Additionally, there are other useful scheduler plugins that can be enabled
alongside the g8s8 functionality.  For example, the coscheduler plugin makes
the default k8s scheduler capable of gang scheduling.  By developing g8s8 as a
scheduler framework plugin, the coscheduler functionality can be enabled
without any additional development work.

## g8s8 design

### Scheduling Framework, simplified

The default k8s scheduler is a one-pod-at-a-time scheduler.  It decides if/where
to place one pod, then if/where to place the next pod, etc.  To speed up
scheduling of many pods, it introduces a "reserved" state where a decision to
place a pod on a node has already been made, but the pod may not have actually
been placed on the node just yet.  Any work to actually place the pod is done
asynchronously while additional pods are scheduled.

The full Scheduling Framework API explanation is [here](
https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework
), but we will only consider a subset of the plugin hooks:

* A pod is popped from the unscheduled queue.  The **Filter** hook is called
  for every node so the plugin can tell the scheduler which nodes should not be
  considered for this pod.

* Nodes which pass the Fitler step are passed to the **Score** hook.

* The best-scoring node is chosen.  The pod will eventually be scheduled onto
  this node.  **Reserve** is called to tell the plugin about the decision.
  Importantly, the Scheduling Framework API guarantees that if Reserve is
  called and there is a subsequent scheduling failure, the **Unreserve** hook
  will be called.  That makes **Reserve** the first safe time to allocate
  resources to scheduling a pod.

* The **PreBind** hook is called in the background.  (The main thread will
  proceed trying to schedule the next unscheduled pod.)  PreBind is where any
  long-running work is done that is needed before the pod is actually bound to
  the node.

### Mapping g8s8 onto the Scheduling Framework

* g8s8 must implement the **Filter** plugin hook to prevent scheduling pods on
  nodes which do not have sufficient IPUs to offer the pod.  This is the key
  network-attached resource tracking which the default scheduler cannot offer.

* g8s8 must implement the **Reserve** plugin hook to track resources allocated.
  Since IPU scheduling is topology-aware, it is necessary that the exact IPUs
  allocated to a job are decided before continuing, as the next call to
  **Filter** for the next unscheduled pod must know exactly which IPUs have
  been reserved.

* g8s8 must implement the **PreBind** plugin to block until the IPU partition
  required by a pod has been successfully created.

### g8s8 required components

* **VIPU client**: for g8s8 to implement the **PreBind** logic, it must be able
  to communicate with the VIPU controllers that actually create the IPU
  partitions.

* **IPU placement engine**: The **Filter** function must be fast.  Network
  calls to the VIPU server would take too long and would slow the k8s
  scheduling cycle too much.  So the IPU placement decisions need to be made
  within g8s8.

* **Multi-VIPU-server IPU Scheduler**: Since there may be multiple IPU POD
  installations within a single g8s8 cluster, g8s8 must be able to handle
  multiple VIPU servers, and do IPU placement and tracking for each of them.
