# "G"raphcore "S"cheduler prototype (g8s8)

`g8s8` is a prototype k8s scheduler for Graphcore hardware.

## Meta Scheduling, or whatever you want to call it

The key feature of g8s8 is what might be called a "meta scheduler".  A k8s
scheduler needs the ability to answer hypothetical questions without actually
scheduling, like, "would this pod fit on this head node?" or "would pod A fit
on this head node if pod B were preempted?".  The VIPU servers are not capable
of this sort of interface, and even if they were the network latency would kill
scheduling performance.  The meta scheduler must be implemented within a single
memory space.

The meta scheduler behavior is implemented within `tracker/tracker.go`, along
with some kubernetes-specific asynchronous behavior.  In the default k8s
scheduler, the fit-finding step is serialized for all pods, while the "binding"
process is allowed to be asynchronous and may be parallelized.  For g8s8, this
means the fit-finding part of the Tracker object is very fast, then the Tracker
marks those IPUs as used (even though the partitions are not actually created
yet).  Then during the k8s scheduler's binding step (the asynchronous
parallelizable part), the Tracker actually creates the partition it
planned out for a pod (or cleans up if partition creation fails).

## Building and running g8s8

First, browse [the devcluster README](
https://github.com/determined-ai/devcluster/tree/master/README.md), a useful
development tool for tasks like this.  Also probably check out the [commented
devcluster example config](
https://github.com/determined-ai/devcluster/blob/master/devcluster/example.yaml
) since the `devcluster.yaml` file in this project's root directory uses quite
a few devcluster features.

Then browse [the Determined k8s development README](
https://github.com/determined-ai/determined/tree/master/tools/k8s/README.md
), from which the these steps are derived.

Then:

  - Make sure `make build` works in the git project root.  Their build system is
    hacky, so this might take some effort.

  - Make sure `virm-agent-fake` docker image is available and the `vipu-server`
    command is available.  I only had 1.17.0 to develop against.

  - Run `kind create cluster` (or `minikube` equivalent) for local k8s
    development.

  - From git project root: `devcluster -c devcluster.yaml` to actually run the
    g8s8 scheduler.

  - Read `sleep.yaml` and submit with `kubectl apply -f sleep.yaml`.
