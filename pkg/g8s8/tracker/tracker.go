package tracker

import (
	"fmt"
	"context"
	"time"
	"sync"

	"github.com/pkg/errors"
	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/scheduler-plugins/pkg/g8s8/protos"
)

func retryLoop(
	immediate bool, period time.Duration, ctx context.Context, fn func()(bool, error),
) error {
	if immediate {
		done, err := fn()
		if done || err != nil {
			return err
		}
	}
	t := time.NewTimer(period)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			done, err := fn()
			if done || err != nil {
				return err
			}
			// try again later
			t.Reset(period)
		}
	}
}

type Partition struct {
	id string
	slotID int
}

// SlotID is a unique number (for a given pool of IPUs) that describes both the placement and size
// of a partition.  The numbering is possible if all partition are power-of-two size and also
// power-of-two aligned.
//
// We can imagine full binary tree of SlotIDs, where the leaves represent individual IPUs, and every
// layer up represents a partition using all the leaves underneath it.  8-IPU illustration:
//
//                   X                  <-- 1x 8-node partition
//                /     \
//           X               X          <-- 2x 4-node partitions
//         /   \           /   \
//       X       X       X       X      <-- 4x 2-node partitions
//      / \     / \     / \     / \
//     X   X   X   X   X   X   X   X    <-- 8x 1-node partitions
//
// We will store the tree as a flat array with a top-down, depth-first numbering scheme that causes
// any subset of the tree to have the same numbering as its peers, with just an offset (compare 1 to
// 8, or 2 to 5 to 9 to 12):
//
//                   0
//                /     \
//           1               8
//         /   \           /   \
//       2       5       9       12
//      / \     / \     / \     /  \
//     3   4   6   7  10   11  13  14
//
// Now, with that numbering scheme, we can store the state of all partitions on the cluster as an
// array of ints, where each int represents how many IPUs are in use within that slotID.  For
// example, if there was a partition at slotID=1 (a 4-IPU partition) and a partition at slotID=11
// (a 1-IPU partition), then our "used" tree would look like:
//
//                   5
//                /     \
//           4               1
//         /   \           /   \
//       0       0       1       0
//      / \     / \     / \     / \
//     0   0   0   0   0   1   0   0
//
// which flattens to:
//
//     used := []int{5, 4, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0}
//
// Note that we propagate usage counts up the tree, but not down the tree (there are zeros under the
// four-node partition).  This keeps modifications to O(log(n)), and is harmless because findFit()
// recurses from the top-down anyway.
//
// I (rb) picked this indexing scheme to try it, and it is well-suited to recursion, but I think it
// may not have been the best choice, and numbering top-down, breadth-first may have been better in
// retrospect.  But since tests are passing and this is just a prototype, I am leaving it as-is.

// returns a slotID or -1 if no fit is found
func findFit(used []int, need int) int {
	total := (len(used)+1) / 2
	if total == need && used[0] == 0 {
		return 0
	}
	if total - used[0] < need {
		return -1
	}
	// child nodes are determined by offsets
	first := 1
	second := total
	childSize := total - 1
	// recurse into the more densly packed child first
	if used[second] > used[first] {
		first, second = second, first
	}
	fit := findFit(used[first:first+childSize], need)
	if fit >= 0 {
		return fit + first
	}
	fit = findFit(used[second:second+childSize], need)
	if fit >= 0 {
		return fit + second
	}
	return -1
}

func appendChildIPUs(size int, pos int, offset int, in []int) []int {
	if size == 1 {
		return append(in, offset)
	}
	tmp := appendChildIPUs(size/2, pos+1, offset, in)
	return appendChildIPUs(size/2, pos+size, offset+size/2, tmp)
}

// TODO: precompute these?
func findIPUs(used []int, slotID int) []int {
	// pos is the slotID we're pointing at
	pos := 0
	// offset is the physical offset of ipu number we are considering
	offset := 0
	size := (len(used)+1) / 2
	ipus := make([]int, 0, size)
	for size > 0 {
		if pos == slotID {
			return appendChildIPUs(size, pos, offset, ipus)
		}
		if slotID < pos + size {
			pos += 1
		} else {
			pos += size
			offset += size/2
		}
		size /= 2
	}
	panic(fmt.Sprintf("findIPUs(slotID=%v) failed!", slotID))
}

// findPath writes a series of slotIDs to path from the root node to the given slotID, returning the
// pathlen and the size of the partition for the slotID.
// TODO: precompute these?
func findPath(used []int, slotID int, path *[16]int) (int, int) {
	pathlen := 0
	pos := 0
	size := (len(used)+1) / 2

	for size > 0 {
		(*path)[pathlen] = pos
		pathlen++
		if pos == slotID {
			// this is our node!
			return pathlen, size
		}
		if slotID < pos + size {
			pos += 1
		} else {
			pos += size
		}
		size /= 2
	}
	panic(fmt.Sprintf("findPath(slotID=%v) failed!", slotID))
}

func setUsed(used []int, slotID int) {
	// support a tree of 2^15 IPUs
	var path [16]int
	pathlen, size := findPath(used, slotID, &path)
	total := (len(used)+1) / 2
	for i := 0 ; i < pathlen ; i++ {
		idx := path[i]
		used[idx] += size
		if used[idx] > total {
			panic("setUsed overflow")
		}
		total /= 2
	}
}

func clearUsed(used []int, slotID int) {
	// support a tree of 2^15 IPUs
	var path [16]int
	pathlen, size := findPath(used, slotID, &path)
	for i := 0 ; i < pathlen ; i++ {
		idx := path[i]
		if used[idx] < size {
			panic("clearUsed underflow")
		}
		used[idx] -= size
	}
}

// Tracker tracks partitions created and in the process of being created for a single VIPU server.
type Tracker struct {
	// vc maps VIPUClients to nodes, which may not be one-to-one.
	vc VIPUClient

	// ctx must only be closed when the whole application is closing
	ctx context.Context

	mutex sync.Mutex

	// how many IPUs under each slotID are in use
	used []int
}

func NewTracker(ctx context.Context, vc VIPUClient, nipus int) *Tracker{
	return &Tracker{ ctx: ctx, vc: vc, used: make([]int, 2*nipus-1) }
}

func (t *Tracker) removePartition(part Partition) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	clearUsed(t.used, part.slotID)
}

// RequestPartition starts a PartitionRequest.
func (t *Tracker) RequestPartition(nipus uint32) *PartitionRequest {
	id := uuid.New().String()

	ctx, cancelFunc := context.WithCancel(t.ctx)

	slotID := func() int {
		// select a slotID and mark it as used within a single lock
		t.mutex.Lock()
		defer t.mutex.Unlock()

		slotID := findFit(t.used, int(nipus))
		if slotID < 0 {
			// RequestPartition must not be called when another RequestPartition call has been made
			// since WouldFit returned true for this (nipus,node) pair.
			panic("findFit failed in RequestPartition")
		}

		setUsed(t.used, slotID)

		return slotID
	}()

	part := Partition{
		id: id,
		slotID: slotID,
	}

	p := &PartitionRequest{
		t: t,
		part: part,
		cancelFunc: cancelFunc,
		done: make(chan struct{}),
	}

	createPartition := func() error {
		// create the partition
		ipus := findIPUs(t.used, slotID)
		err := t.vc.CreatePartition(ctx, id, ipus)
		if err != nil {
			return errors.Wrap(err, "CreatePartition")
		}

		checkReady := func()(bool, error) {
			// check status
			status, err := t.vc.GetPartitionStatus(ctx, id)
			if err != nil {
				return false, errors.Wrap(err, "GetPartitionStatus")
			}
			switch status {
			case "pending":
				// try again later
				return false, nil
			case "active":
				// success!
				return true, nil
			default:
				return false, errors.Errorf("unexpected status: %q", status)
			}
		}

		return retryLoop(false, 500 * time.Millisecond, ctx, checkReady)
	}

	go func() {
		defer func() {
			if p.err != nil {
				// if creation failed, ensure the partition is deleted
				t.EnsureDeleted(part)
			}
		}()
		defer close(p.done)
		p.err = createPartition()
	}()

	return p
}

func (t *Tracker) EnsureDeleted(part Partition) {
	defer t.removePartition(part)

	retries := 1000

	retry := func(err error){
		retries--
		if retries == 0 {
			panic(fmt.Sprintf("RemovePartitionRequest failed 1000 times: %q", err.Error()))
		}
	}

	notFound := false

	rm := func()(bool, error) {
		exists, err := t.vc.RemovePartition(t.ctx, part.id)
		if err != nil {
			retry(err)
			return false, nil
		}
		if !exists {
			notFound = true
		}
		return true, nil
	}

	period := 500 * time.Millisecond

	// rm never returns an error
	_ = retryLoop(true, period, t.ctx, rm)

	if notFound {
		return
	}

	checkDeletion := func()(bool, error) {
		status, err := t.vc.GetPartitionStatus(t.ctx, part.id)
		if err != nil {
			retry(err)
			return false, nil
		}
		if status == "notfound" {
			return true, nil
		}
		return false, nil
	}

	// checkDeletion never returns an error
	_ = retryLoop(true, period, t.ctx, checkDeletion)
}

// WouldFit predicts if a partition of a given size would fit on a VIPU server.
func (t *Tracker) WouldFit(nipus uint32) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return findFit(t.used, int(nipus)) >= 0
}

// PartitionRequest represents an background effort to create a partition.
type PartitionRequest struct {
	t *Tracker
	part Partition
	cancelFunc func()
	done chan struct{}
	err error
}

// Await waits for the partition to finish being created, or to fail.
func (p *PartitionRequest) Await(ctx context.Context) (Partition, error) {
	select {
	case <-p.done:
		// Partition creation finished, though it may have failed.
		return p.part, p.err
	case <-ctx.Done():
		// Whoever was awaiting doesn't care anymore.
		return p.part, ctx.Err()
	}
}

// Cancel stops the partition creation, or deletes it if it already finished successfully.
// XXX a couple problems:
// XXX  - Two Cancels at the same time would conflict
// XXX  - Cancel returns a chan that might be closed before EnsureDeleted is done, if EnsureDeleted
//        is called from a different thread (if p.err != nil)
func (p *PartitionRequest) Cancel() chan struct{} {
	cancelDone := make(chan struct{})
	// try to cancel creation
	p.cancelFunc()
	go func() {
		defer close(cancelDone)
		// wait for creation finish
		<-p.done
		// if it succeeded we should delete it
		if p.err == nil {
			p.t.EnsureDeleted(p.part)
		}
	}()
	return cancelDone
}

// VIPUClient is an interface for easy mocking
type VIPUClient interface {
	// CreatePartition creates a partition.
	CreatePartition(ctx context.Context, id string, ipus []int) error
	// RemovePartition deletes a partition, returning if such a partition exists.
	RemovePartition(ctx context.Context, id string) (bool, error)
	// GetPartitionStatus returns "pending", "active", "notfound", or an error.
	GetPartitionStatus(ctx context.Context, id string) (string, error)
}

// RealVIPUClient uses actual protos
type RealVIPUClient struct {
	Version protos.VersionServiceClient
	User protos.UserServiceClient
	Allocation string
}

func (vc *RealVIPUClient) CreatePartition(ctx context.Context, id string, ipus []int) error {
	reqIPUs := make([]*protos.PartitionIpu, 0, len(ipus))
	for _, ipu := range ipus {
		reqIPUs = append(reqIPUs, &protos.PartitionIpu{
			TopologyId: uint32(ipu),
			RoutingId: uint32(ipu - ipus[0]),
		})
	}

	reqs := protos.PartitionRequirements{
		Id: id,
		Size: uint32(len(ipus)),
		AllocationId: vc.Allocation,
		Ipus: reqIPUs,
	}
	req := protos.CreatePartitionRequest{ Requirements: &reqs }
	_, err := vc.User.CreatePartition(ctx, &req)
	return err
}

func (vc *RealVIPUClient) RemovePartition(ctx context.Context, id string) (bool, error) {
	req := protos.RemovePartitionRequest{ PartitionId: id, Force: true }
	_, err := vc.User.RemovePartition(ctx, &req)
	if err != nil {
		s, ok := status.FromError(err)
		if ok && s.Code() == codes.NotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (vc *RealVIPUClient) GetPartitionStatus(ctx context.Context, id string) (string, error) {
	req := protos.GetPartitionStatusRequest{ PartitionId: id }
	resp, err := vc.User.GetPartitionStatus(ctx, &req)
	if err != nil {
		s, ok := status.FromError(err)
		if ok && s.Code() == codes.NotFound {
			return "notfound", nil
		}
		return "", err
	}
	status := resp.GetStatus()
	switch status.GetReadyState() {
	case protos.PartitionReadyState_PS_PENDING:
		return "pending", nil
	case protos.PartitionReadyState_PS_ACTIVE:
		return "active", nil
	}
	return "", errors.Errorf("unexpected partitions state: %q", status.String())
}
