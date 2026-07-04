package test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
)

// checkStoresAgree waits for every node's state machine to converge to the same
// contents (State Machine Safety across command + snapshot applies).
func (c *cluster) checkStoresAgree() {
	c.t.Helper()
	for iter := 0; iter < 40; iter++ {
		ref := c.stores[0].Dump()
		agree := true
		for i := 1; i < c.n; i++ {
			if !reflect.DeepEqual(ref, c.stores[i].Dump()) {
				agree = false
				break
			}
		}
		if agree {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	for i := 0; i < c.n; i++ {
		c.t.Logf("node %d store: %v", i, c.stores[i].Dump())
	}
	c.t.Fatalf("state machines did not converge")
}

// maxLogSize is the largest uncompacted log across all nodes.
func (c *cluster) maxLogSize() uint64 {
	var m uint64
	for i := 0; i < c.n; i++ {
		if s := c.rafts[i].LogSize(); s > m {
			m = s
		}
	}
	return m
}

// TestSnapshotBoundsLog: under sustained writes with snapshotting enabled, the
// log stays bounded (compaction happens) and all state machines agree.
func TestSnapshotBoundsLog(t *testing.T) {
	const threshold = 1000
	c := makeClusterBoltSnap(t, 3, 301, true, threshold)
	defer c.cleanup()
	c.checkOneLeader()

	for i := 0; i < 200; i++ {
		c.one(kv.EncodePut(fmt.Sprintf("k%d", i%10), fmt.Sprintf("v%d", i)), 3, true)
	}
	time.Sleep(300 * time.Millisecond) // let the last compactions land

	if sz := c.maxLogSize(); sz > threshold*4 {
		t.Fatalf("log not bounded under snapshotting: max %d bytes (threshold %d)", sz, threshold)
	}
	c.checkStoresAgree()
}

// TestInstallSnapshotCatchup: a follower that falls behind far enough that the
// leader has compacted its needed prefix is caught up via InstallSnapshot.
func TestInstallSnapshotCatchup(t *testing.T) {
	const threshold = 1000
	c := makeClusterBoltSnap(t, 3, 302, true, threshold)
	defer c.cleanup()
	leader := c.checkOneLeader()

	f := (leader + 1) % 3
	c.disconnect(f)
	for i := 0; i < 120; i++ { // majority keeps committing; leader compacts past f
		c.one(kv.EncodePut(fmt.Sprintf("k%d", i%5), fmt.Sprintf("v%d", i)), 2, true)
	}
	time.Sleep(200 * time.Millisecond)

	c.connect(f)
	// f can only reach parity via InstallSnapshot; then this commits on all 3.
	c.one(kv.EncodePut("final", "ok"), 3, true)
	c.checkStoresAgree()
	if v, _ := c.stores[f].Dump()["final"]; v != "ok" {
		t.Fatalf("caught-up follower missing final write: %q", v)
	}
}

// TestRestartFromSnapshot: a node that crashed after snapshotting rebuilds its
// state machine from the on-disk snapshot (plus any log tail) on restart.
func TestRestartFromSnapshot(t *testing.T) {
	const threshold = 1000
	c := makeClusterBoltSnap(t, 3, 303, true, threshold)
	defer c.cleanup()
	c.checkOneLeader()

	for i := 0; i < 80; i++ {
		c.one(kv.EncodePut(fmt.Sprintf("k%d", i%8), fmt.Sprintf("v%d", i)), 3, true)
	}
	time.Sleep(200 * time.Millisecond) // ensure a snapshot exists on disk

	c.crashAndRestart(1) // must restore from snapshot, not replay a full log
	c.one(kv.EncodePut("after", "restart"), 3, true)
	c.checkStoresAgree()
}
