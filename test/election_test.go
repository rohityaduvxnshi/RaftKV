package test

import (
	"testing"
	"time"
)

// TestInitialElection: a fresh 3-node cluster elects exactly one leader.
func TestInitialElection(t *testing.T) {
	c := makeCluster(t, 3, 1, true)
	defer c.cleanup()
	c.checkOneLeader()
}

// TestElection5Nodes: a 5-node cluster also elects exactly one leader.
func TestElection5Nodes(t *testing.T) {
	c := makeCluster(t, 5, 7, true)
	defer c.cleanup()
	c.checkOneLeader()
}

// TestTermAgreement: once a leader is established, every connected node adopts
// the leader's term (term propagation via heartbeats).
func TestTermAgreement(t *testing.T) {
	c := makeCluster(t, 5, 11, true)
	defer c.cleanup()
	leader := c.checkOneLeader()
	agreed := c.checkTerms()
	if lt := c.term(leader); lt != agreed {
		t.Fatalf("leader term %d != agreed term %d", lt, agreed)
	}
}

// TestReElection: killing the leader elects a new one in a higher term, the old
// leader rejoining doesn't create a second leader, a minority cannot elect, and
// restoring quorum elects again. Exercises Election Safety throughout.
func TestReElection(t *testing.T) {
	c := makeCluster(t, 3, 3, true)
	defer c.cleanup()

	leader1 := c.checkOneLeader()

	// Isolate the leader — the other two must elect a new one in a higher term.
	termBefore := c.term(leader1)
	c.disconnect(leader1)
	leader2 := c.checkOneLeader()
	if leader2 == leader1 {
		t.Fatalf("isolated leader %d is still leading", leader1)
	}
	if c.term(leader2) <= termBefore {
		t.Fatalf("new leader term %d not higher than old %d", c.term(leader2), termBefore)
	}

	// Old leader rejoins: it should step down; still exactly one leader.
	c.connect(leader1)
	c.checkOneLeader()

	// Drop to a minority (only 1 of 3 reachable): no leader can be elected.
	other := (leader2 + 1) % 3
	if other == leader2 {
		other = (leader2 + 2) % 3
	}
	c.disconnect(leader2)
	c.disconnect(other)
	time.Sleep(400 * time.Millisecond) // > max election timeout
	c.checkNoLeader()

	// Restore quorum: a leader emerges again.
	c.connect(other)
	c.checkOneLeader()
}

// TestElectionUnreliable: under ~10% message drop and random delays a single
// STABLE leader emerges and stays put. The fault *decisions* are seeded (see the
// inmem package docs); goroutine scheduling is not, so this asserts a property
// (bounded term growth over a sustained window), not an exact interleaving. The
// bound is a regression guard against gross leader churn.
func TestElectionUnreliable(t *testing.T) {
	c := makeCluster(t, 5, 42, false)
	defer c.cleanup()

	c.checkOneLeader()
	termBefore := c.maxTerm()

	// Keep running under loss; a correct cluster converges and holds. Occasional
	// re-elections are within Raft's design envelope, but the term must not run
	// away.
	time.Sleep(900 * time.Millisecond)

	c.checkOneLeader() // Election Safety still holds; a leader still exists.
	termAfter := c.maxTerm()
	growth := termAfter - termBefore
	t.Logf("term growth over stability window: %d -> %d (+%d)", termBefore, termAfter, growth)
	if growth > 6 {
		t.Fatalf("term inflated by %d (%d -> %d) under stable load — leader churn", growth, termBefore, termAfter)
	}
}

// TestNoChurnOnRejoin: a follower isolated long enough to advance its term far
// ahead of the leader, then reconnected, must let the cluster reconcile to a
// single leader with a clean, single election — not churn the term. A guard for
// partition-heal stability (and the step-down timer-reset behavior), though in
// Phase 1 the inbound higher-term RequestVote usually resets timers before the
// reply path can, so this passes with or without that fix.
func TestNoChurnOnRejoin(t *testing.T) {
	c := makeCluster(t, 3, 5, true)
	defer c.cleanup()
	leader := c.checkOneLeader()

	f := (leader + 1) % 3
	c.disconnect(f)
	time.Sleep(1200 * time.Millisecond) // f times out repeatedly, term climbs
	fTerm := c.term(f)                  // GetState works even while partitioned

	c.connect(f)
	time.Sleep(1200 * time.Millisecond) // let the cluster reconcile

	c.checkOneLeader()
	settled := c.maxTerm()
	delta := settled - fTerm
	t.Logf("isolated term=%d, settled term=%d, delta=%d", fTerm, settled, delta)
	if delta > 3 {
		t.Fatalf("term churned by %d after rejoin (isolated %d -> settled %d) — leader fight", delta, fTerm, settled)
	}
}
