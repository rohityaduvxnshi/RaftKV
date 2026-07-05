package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
)

// applyAll replays the agreed committed log (in index order) into a fresh KV
// store, returning the canonical state. It also asserts Log Matching: the
// committed prefix has no gaps.
func (c *cluster) applyAll() *kv.Store {
	c.amu.Lock()
	defer c.amu.Unlock()
	var max uint64
	for idx := range c.committed {
		if idx > max {
			max = idx
		}
	}
	st := kv.New()
	for i := uint64(1); i <= max; i++ {
		if cmd, ok := c.committed[i]; ok {
			st.Apply(cmd)
			continue
		}
		if _, isNoop := c.noops[i]; isNoop {
			continue // election barrier, no state change
		}
		c.t.Fatalf("gap in committed log at index %d (Log Matching violated)", i)
	}
	return st
}

// TestSingleNode: a one-node cluster elects itself and commits + applies its own
// writes (no follower reply exists to drive commit, so the leader must advance
// its own commitIndex).
func TestSingleNode(t *testing.T) {
	c := makeCluster(t, 1, 100, true)
	defer c.cleanup()
	c.checkOneLeader()
	c.one(kv.EncodePut("solo", "ok"), 1, true)
	if v, _ := c.applyAll().Get("solo"); v != "ok" {
		t.Fatalf("solo=%q, want ok", v)
	}
}

// TestBasicAgreement: writes through the leader replicate to every node and land
// at contiguous, in-order indices.
func TestBasicAgreement(t *testing.T) {
	c := makeCluster(t, 3, 101, true)
	defer c.cleanup()
	c.checkOneLeader()
	var prev uint64
	for i := 1; i <= 5; i++ {
		idx := c.one(kv.EncodePut("k", fmt.Sprintf("v%d", i)), c.n, true)
		if prev != 0 && idx != prev+1 {
			t.Fatalf("writes landed at non-contiguous indices: %d then %d", prev, idx)
		}
		prev = idx
	}
}

// TestAgreementWithFollowerDown: a majority keeps committing with one follower
// partitioned; the follower catches up on reconnect.
func TestAgreementWithFollowerDown(t *testing.T) {
	c := makeCluster(t, 3, 102, true)
	defer c.cleanup()
	leader := c.checkOneLeader()
	c.one(kv.EncodePut("a", "1"), 3, true)

	f := (leader + 1) % 3
	c.disconnect(f)
	c.one(kv.EncodePut("a", "2"), 2, true) // majority of 2/3 still commits
	c.one(kv.EncodePut("a", "3"), 2, true)

	c.connect(f)
	c.one(kv.EncodePut("a", "4"), 3, true) // follower caught up: all 3 commit
}

// TestLeaderChangeKeepsCommitted: after the leader is lost mid-workload, no
// committed entry is lost or reordered and the cluster keeps making progress.
func TestLeaderChangeKeepsCommitted(t *testing.T) {
	c := makeCluster(t, 5, 103, true)
	defer c.cleanup()

	c.one(kv.EncodePut("x", "1"), 5, true)
	c.one(kv.EncodePut("x", "2"), 5, true)

	leader := c.checkOneLeader()
	c.disconnect(leader) // lose the leader

	c.one(kv.EncodePut("x", "3"), 4, true) // remaining majority elects + commits
	c.one(kv.EncodePut("x", "4"), 4, true)

	c.connect(leader) // old leader rejoins and reconciles
	c.one(kv.EncodePut("x", "5"), 5, true)

	if v, _ := c.applyAll().Get("x"); v != "5" {
		t.Fatalf("x=%q, want 5", v)
	}
}

// TestDeposedLeaderEntriesOverwritten: uncommitted entries appended by an
// isolated (deposed) leader are overwritten once it rejoins. The drain's State
// Machine Safety check fails loudly if any node applies a stale entry.
func TestDeposedLeaderEntriesOverwritten(t *testing.T) {
	c := makeCluster(t, 3, 104, true)
	defer c.cleanup()
	leader1 := c.checkOneLeader()
	c.one(kv.EncodePut("k", "committed"), 3, true) // index 1 on everyone

	// Isolate the leader and feed it writes that can never commit (no majority).
	c.disconnect(leader1)
	for i := 0; i < 3; i++ {
		c.rafts[leader1].Submit(kv.EncodePut("k", fmt.Sprintf("stale%d", i)))
	}

	// The other two elect a new leader and commit a different entry at index 2.
	c.one(kv.EncodePut("k", "winner"), 2, true)

	// Rejoin: leader1's uncommitted entries must be overwritten by "winner".
	c.connect(leader1)
	c.one(kv.EncodePut("k", "after"), 3, true)

	if v, _ := c.applyAll().Get("k"); v != "after" {
		t.Fatalf("k=%q, want after", v)
	}
}

// TestConcurrentSubmits: many concurrent writers all commit; the drain asserts
// each node applies them in the same order with no divergence.
func TestConcurrentSubmits(t *testing.T) {
	c := makeCluster(t, 3, 105, true)
	defer c.cleanup()
	c.checkOneLeader()

	const writers = 25
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.one(kv.EncodePut(fmt.Sprintf("k%d", i), "v"), c.n, true)
		}(i)
	}
	wg.Wait()

	st := c.applyAll()
	for i := 0; i < writers; i++ {
		if _, ok := st.Get(fmt.Sprintf("k%d", i)); !ok {
			t.Fatalf("k%d missing from committed state", i)
		}
	}
}

// TestKVStateMachine: Put/CAS/Delete apply deterministically through the log.
func TestKVStateMachine(t *testing.T) {
	c := makeCluster(t, 3, 106, true)
	defer c.cleanup()
	c.checkOneLeader()

	c.one(kv.EncodePut("x", "1"), 3, true)
	c.one(kv.EncodePut("y", "2"), 3, true)
	c.one(kv.EncodeCAS("x", "1", "10"), 3, true) // matches -> swaps to 10
	c.one(kv.EncodeCAS("x", "1", "99"), 3, true) // stale expected -> no-op
	c.one(kv.EncodeCAS("z", "", "new"), 3, true) // absent key -> no-op (CAS requires existence)
	c.one(kv.EncodeDelete("y"), 3, true)

	st := c.applyAll()
	if v, _ := st.Get("x"); v != "10" {
		t.Fatalf("x=%q, want 10", v)
	}
	if _, ok := st.Get("y"); ok {
		t.Fatalf("y should have been deleted")
	}
	if _, ok := st.Get("z"); ok {
		t.Fatalf("z should not exist (CAS on absent key must be a no-op)")
	}
}
