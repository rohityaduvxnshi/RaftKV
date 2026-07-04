package test

import (
	"fmt"
	"testing"

	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
)

// appliedCount reports how many committed entries node i has applied (used to
// confirm a recovered node replays its whole log).
func (c *cluster) appliedCount(i int) int {
	c.amu.Lock()
	defer c.amu.Unlock()
	return len(c.applied[i])
}

// TestFollowerCrashRecovery: a follower killed and restarted recovers its log
// from disk and rejoins; a subsequent write commits on all nodes.
func TestFollowerCrashRecovery(t *testing.T) {
	c := makeClusterBolt(t, 3, 201, true)
	defer c.cleanup()
	leader := c.checkOneLeader()
	for i := 1; i <= 3; i++ {
		c.one(kv.EncodePut("k", fmt.Sprintf("v%d", i)), 3, true)
	}

	f := (leader + 1) % 3
	c.crashAndRestart(f)

	c.one(kv.EncodePut("k", "v4"), 3, true) // needs f caught up to reach 3/3
	if got := c.appliedCount(f); got != 4 {
		t.Fatalf("recovered follower applied %d entries, want 4 (log not fully recovered)", got)
	}
	if v, _ := c.applyAll().Get("k"); v != "v4" {
		t.Fatalf("k=%q, want v4", v)
	}
}

// TestLeaderCrashRecovery: killing and restarting the leader loses no committed
// data; the cluster re-elects and keeps serving.
func TestLeaderCrashRecovery(t *testing.T) {
	c := makeClusterBolt(t, 5, 202, true)
	defer c.cleanup()
	leader := c.checkOneLeader()
	for i := 1; i <= 4; i++ {
		c.one(kv.EncodePut("k", fmt.Sprintf("v%d", i)), 5, true)
	}

	c.crashAndRestart(leader)

	c.checkOneLeader()
	c.one(kv.EncodePut("k", "v5"), 5, true)
	if v, _ := c.applyAll().Get("k"); v != "v5" {
		t.Fatalf("k=%q, want v5", v)
	}
}

// TestWholeClusterRestart: the whole cluster crashes and restarts; committed
// data survives entirely from disk, and every node replays it.
func TestWholeClusterRestart(t *testing.T) {
	c := makeClusterBolt(t, 3, 203, true)
	defer c.cleanup()
	c.checkOneLeader()
	c.one(kv.EncodePut("a", "1"), 3, true)
	c.one(kv.EncodePut("b", "2"), 3, true)
	c.one(kv.EncodePut("a", "3"), 3, true) // a: 1 -> 3

	c.crashAllAndRestart()

	// Re-elect, then a fresh write re-commits the recovered prior-term entries.
	c.checkOneLeader()
	c.one(kv.EncodePut("c", "4"), 3, true)

	st := c.applyAll()
	if v, _ := st.Get("a"); v != "3" {
		t.Fatalf("a=%q, want 3 (recovered)", v)
	}
	if v, _ := st.Get("b"); v != "2" {
		t.Fatalf("b=%q, want 2 (recovered)", v)
	}
	if v, _ := st.Get("c"); v != "4" {
		t.Fatalf("c=%q, want 4", v)
	}
	for i := 0; i < c.n; i++ {
		if got := c.appliedCount(i); got != 4 {
			t.Fatalf("node %d applied %d entries after restart, want 4", i, got)
		}
	}
}

// TestSingleNodeCrashRecovery: an N=1 cluster's committed writes survive a crash.
func TestSingleNodeCrashRecovery(t *testing.T) {
	c := makeClusterBolt(t, 1, 204, true)
	defer c.cleanup()
	c.checkOneLeader()
	c.one(kv.EncodePut("solo", "before"), 1, true)

	c.crashAndRestart(0)

	c.checkOneLeader()
	c.one(kv.EncodePut("solo2", "after"), 1, true) // re-commits the recovered entry too
	st := c.applyAll()
	if v, _ := st.Get("solo"); v != "before" {
		t.Fatalf("solo=%q, want before (recovered from disk)", v)
	}
	if v, _ := st.Get("solo2"); v != "after" {
		t.Fatalf("solo2=%q, want after", v)
	}
}
