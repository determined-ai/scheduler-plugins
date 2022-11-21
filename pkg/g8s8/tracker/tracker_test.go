package tracker

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"context"
	"sync"

	"gotest.tools/assert"
	"github.com/pkg/errors"
)

func TestFindIPUs(t *testing.T) {
	used := make([]int, 15)

	type TestCase struct {
		SlotID int
		Exp []int
	}

	cases := []TestCase{
		TestCase{0,  []int{0, 1, 2, 3, 4, 5, 6, 7}},
		TestCase{1,  []int{0, 1, 2, 3}},
		TestCase{2,  []int{0, 1}},
		TestCase{3,  []int{0}},
		TestCase{4,  []int{1}},
		TestCase{5,  []int{2, 3}},
		TestCase{6,  []int{2}},
		TestCase{7,  []int{3}},
		TestCase{8,  []int{4, 5, 6, 7}},
		TestCase{9,  []int{4, 5}},
		TestCase{10, []int{4}},
		TestCase{11, []int{5}},
		TestCase{12, []int{6, 7}},
		TestCase{13, []int{6}},
		TestCase{14, []int{7}},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("findIPUs(SlotID=%v)", tc.SlotID)
		t.Run(name, func(t *testing.T){
			ipus := findIPUs(used, tc.SlotID)
			assert.DeepEqual(t, ipus, tc.Exp)
		})
	}
}

func TestFindPath(t *testing.T) {
	used := make([]int, 15)

	type TestCase struct {
		SlotID int
		ExpSize int
		ExpPath []int
	}

	cases := []TestCase{
		TestCase{0,  8, []int{0}},
		TestCase{1,  4, []int{0, 1}},
		TestCase{2,  2, []int{0, 1, 2}},
		TestCase{3,  1, []int{0, 1, 2, 3}},
		TestCase{4,  1, []int{0, 1, 2, 4}},
		TestCase{5,  2, []int{0, 1, 5}},
		TestCase{6,  1, []int{0, 1, 5, 6}},
		TestCase{7,  1, []int{0, 1, 5, 7}},
		TestCase{8,  4, []int{0, 8}},
		TestCase{9,  2, []int{0, 8, 9}},
		TestCase{10, 1, []int{0, 8, 9, 10}},
		TestCase{11, 1, []int{0, 8, 9, 11}},
		TestCase{12, 2, []int{0, 8, 12}},
		TestCase{13, 1, []int{0, 8, 12, 13}},
		TestCase{14, 1, []int{0, 8, 12, 14}},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("findPath(SlotID=%v)", tc.SlotID)
		t.Run(name, func(t *testing.T){
			var path [16]int
			pathlen, size := findPath(used, tc.SlotID, &path)
			assert.DeepEqual(t, size, tc.ExpSize)
			pathSlice := path[0:pathlen]
			assert.DeepEqual(t, pathSlice, tc.ExpPath)
		})
	}
}

// convert a human-readable string showing a 4-depth tree into our left-first indexed array
func readTreeString(in string) []int {
	used := make([]int, 15)
	/* index of each position in the tree
	            0
	       1          8
	    2    5     9      12
	   3 4  6 7  10 11  13  14 */
	idxmap := []int{0, 1, 8, 2, 5, 9, 12, 3, 4, 6, 7, 10, 11, 13, 14}
	words := strings.Split(in, " ")
	count := 0
	for _, w := range words {
		w = strings.Trim(w, "\n\r\t")
		if w == "" {
			continue
		}
		n, err := strconv.Atoi(w)
		if err != nil {
			panic(fmt.Sprintf("bad test input, unable to convert %q to an int", w))
		}
		used[idxmap[count]] = n
		count++
	}
	if count != 15 {
		panic(fmt.Sprintf("bad test input, not enough entries!"))
	}
	return used
}

func TestFitting(t *testing.T) {
	type FitCase struct {
		In int
		Out int
	}
	type TestCase struct {
		Clear []int
		Set []int
		ExpUsed string
		Fit8 int
		Fit4 int
		Fit2 int
		Fit1 int
	}

	used := make([]int, 15)

	/* index of each position in the tree
	            0
	       1          8
	    2    5     9      12
	   3 4  6 7  10 11  13  14 */

	cases := []TestCase{
		TestCase{
			Clear: []int{},
			Set: []int{},
			ExpUsed: `
			            0
			      0           0
			   0     0     0     0
			  0 0   0 0   0 0   0 0
			`,
			Fit8: 0,
			Fit4: 1,
			Fit2: 2,
			Fit1: 3,
		},
		TestCase{
			Clear: []int{},
			Set: []int{3, 5, 8},
			ExpUsed: `
			            7
			      3           4
			   1     2     0     0
			  1 0   0 0   0 0   0 0
			`,
			Fit8: -1,
			Fit4: -1,
			Fit2: -1,
			Fit1: 4,
		},
		TestCase{
			Clear: []int{5},
			Set: []int{},
			ExpUsed: `
			            5
			      1           4
			   1     0     0     0
			  1 0   0 0   0 0   0 0
			`,
			Fit8: -1,
			Fit4: -1,
			Fit2: 5,
			Fit1: 4,
		},
		TestCase{
			Clear: []int{8},
			Set: []int{6},
			ExpUsed: `
			            2
			      2           0
			   1     1     0     0
			  1 0   1 0   0 0   0 0
			`,
			Fit8: -1,
			Fit4: 8,
			Fit2: 9,
			Fit1: 4,
		},
		TestCase{
			Clear: []int{},
			Set: []int{4, 7, 13},
			ExpUsed: `
			            5
			      4           1
			   2     2     0     1
			  1 1   1 1   0 0   1 0
			`,
			Fit8: -1,
			Fit4: -1,
			Fit2: 9,
			Fit1: 14,
		},
	}

	for i, tc := range cases {
		name := fmt.Sprintf("case[%v]", i)
		expUsed := readTreeString(tc.ExpUsed)
		t.Run(name, func(t *testing.T) {
			// Clear what the test case requires.
			for _, c := range tc.Clear {
				clearUsed(used, c)
			}
			// Set what the test case requires.
			for _, s := range tc.Set {
				setUsed(used, s)
			}
			// Expect slots to be correct.
			assert.DeepEqual(t, used, expUsed)
			// Run the fit tests.
			assert.DeepEqual(t, tc.Fit8, findFit(used, 8))
			assert.DeepEqual(t, tc.Fit4, findFit(used, 4))
			assert.DeepEqual(t, tc.Fit2, findFit(used, 2))
			assert.DeepEqual(t, tc.Fit1, findFit(used, 1))
		})
	}
}

type MockVIPUClient struct {
	mutex sync.Mutex
	ipus []bool
	active map[string][]int
	pending map[string]bool
	deleting map[string]bool
}

func NewMockVIPUClient(nipus int) *MockVIPUClient {
	return &MockVIPUClient{
		ipus: make([]bool, nipus),
		active: map[string][]int{},
		pending: map[string]bool{},
		deleting: map[string]bool{},
	}
}

func (m *MockVIPUClient) CreatePartition(ctx context.Context, id string, ipus []int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// validate
	for _, ipu := range ipus {
		if m.ipus[ipu] {
			return errors.Errorf("IPU already in use: %d", ipu)
		}
	}

	// accept
	cpy := make([]int, 0, len(ipus))
	for _, ipu := range ipus {
		m.ipus[ipu] = true
		cpy = append(cpy, ipu)
	}

	m.active[id] = cpy
	m.pending[id] = true

	return nil
}

func (m *MockVIPUClient) RemovePartition(ctx context.Context, id string) (bool, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// locate partition
	ipus, ok := m.active[id]
	if !ok {
		return false, nil
	}

	// clear partition
	for _, ipu := range ipus {
		m.ipus[ipu] = false
	}

	delete(m.active, id)
	m.deleting[id] = true

	return true, nil
}

func (m *MockVIPUClient) GetPartitionStatus(ctx context.Context, id string) (string, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// is partition pretending to be pending?
	if _, ok := m.pending[id]; ok {
		delete(m.pending, id)
		return "pending", nil
	}

	// is partition pretending to be deleting?
	if _, ok := m.deleting[id]; ok {
		delete(m.deleting, id)
		return "deleting", nil
	}

	// does an active partition exist?
	if _, ok := m.active[id]; ok {
		return "active", nil
	}

	return "notfound", nil
}

func TestTracker(t *testing.T) {
	tracker := NewTracker(context.Background(), NewMockVIPUClient(8), 8)
	assert.Assert(t, tracker.WouldFit(8))
	assert.Assert(t, tracker.WouldFit(4))
	assert.Assert(t, tracker.WouldFit(2))
	assert.Assert(t, tracker.WouldFit(1))

	preq1 := tracker.RequestPartition(1)

	assert.Assert(t, !tracker.WouldFit(8))
	assert.Assert(t, tracker.WouldFit(4))
	assert.Assert(t, tracker.WouldFit(2))
	assert.Assert(t, tracker.WouldFit(1))

	preq4 := tracker.RequestPartition(4)

	assert.Assert(t, !tracker.WouldFit(8))
	assert.Assert(t, !tracker.WouldFit(4))
	assert.Assert(t, tracker.WouldFit(2))
	assert.Assert(t, tracker.WouldFit(1))

	preq2 := tracker.RequestPartition(2)

	assert.Assert(t, !tracker.WouldFit(8))
	assert.Assert(t, !tracker.WouldFit(4))
	assert.Assert(t, !tracker.WouldFit(2))
	assert.Assert(t, tracker.WouldFit(1))

	cancelDone := preq4.Cancel()
	part1, err := preq1.Await(context.Background())
	assert.NilError(t, err)
	part2, err := preq2.Await(context.Background())
	assert.NilError(t, err)

	<-cancelDone
	assert.Assert(t, tracker.WouldFit(4))

	tracker.EnsureDeleted(part1)
	assert.Assert(t, tracker.WouldFit(2))

	tracker.EnsureDeleted(part2)
	assert.Assert(t, tracker.WouldFit(8))
}
