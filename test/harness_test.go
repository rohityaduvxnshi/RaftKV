package test

import (
	"testing"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	"github.com/rohityaduvxnshi/RaftKV/internal/transport/inmem"
)

// cluster is an N-node Raft test cluster on the simulated in-mem network. All
// harness methods are called only from the test goroutine.
type cluster struct {
	t          *testing.T
	n          int
	seed       int64
	net        *inmem.Network
	rafts      []*raft.Raft
	persisters []*raft.MemPersister
	connected  []bool
}

// makeCluster builds and starts an N-node cluster. Pass reliable=false to inject
// message drops and delays. Everything is reproducible from seed.
func makeCluster(t *testing.T, n int, seed int64, reliable bool) *cluster {
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	c := &cluster{
		t:          t,
		n:          n,
		seed:       seed,
		net:        inmem.NewNetwork(seed),
		rafts:      make([]*raft.Raft, n),
		persisters: make([]*raft.MemPersister, n),
		connected:  make([]bool, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewMemPersister()
		c.rafts[i] = raft.New(raft.Config{
			ID:        i,
			Peers:     peers,
			Transport: c.net.Transport(i),
			Persister: c.persisters[i],
			Seed:      seed,
		})
		c.net.Register(i, c.rafts[i])
		c.connected[i] = true
	}
	for i := 0; i < n; i++ {
		c.rafts[i].Start()
	}
	return c
}

func (c *cluster) cleanup() {
	for _, r := range c.rafts {
		r.Kill()
	}
}

// disconnect / connect partition and heal a node.
func (c *cluster) disconnect(i int) {
	c.connected[i] = false
	c.net.SetConnected(i, false)
}

func (c *cluster) connect(i int) {
	c.connected[i] = true
	c.net.SetConnected(i, true)
}

// checkOneLeader asserts Election Safety — no two leaders share a term — and
// returns the leader of the highest term among connected nodes. It retries to
// let an in-progress election settle.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iters := 0; iters < 12; iters++ {
		time.Sleep(60 * time.Millisecond)
		leadersByTerm := map[uint64][]int{}
		for i := 0; i < c.n; i++ {
			if !c.connected[i] {
				continue
			}
			if term, isLeader := c.rafts[i].GetState(); isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], i)
			}
		}
		best := uint64(0)
		for term, leaders := range leadersByTerm {
			if len(leaders) > 1 {
				c.t.Fatalf("term %d has %d leaders %v — Election Safety violated", term, len(leaders), leaders)
			}
			if term > best {
				best = term
			}
		}
		if len(leadersByTerm) != 0 {
			return leadersByTerm[best][0]
		}
	}
	c.t.Fatalf("expected one leader among connected nodes, got none")
	return -1
}

// checkNoLeader fails if any connected node believes it is leader.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i := 0; i < c.n; i++ {
		if !c.connected[i] {
			continue
		}
		if _, isLeader := c.rafts[i].GetState(); isLeader {
			c.t.Fatalf("node %d is leader but expected none", i)
		}
	}
}

func (c *cluster) term(i int) uint64 {
	t, _ := c.rafts[i].GetState()
	return t
}

// maxTerm returns the highest currentTerm among connected nodes. Rapid growth
// over a stable window is the signature of election churn.
func (c *cluster) maxTerm() uint64 {
	var m uint64
	for i := 0; i < c.n; i++ {
		if !c.connected[i] {
			continue
		}
		if t := c.term(i); t > m {
			m = t
		}
	}
	return m
}

// checkTerms asserts that all connected nodes converge on a single term (the
// leader's), verifying term propagation via heartbeats. It retries to let
// followers adopt the leader's term, then returns the agreed term. Meaningful
// only on a reliable network, where a healthy leader keeps followers from
// timing out and transiently diverging.
func (c *cluster) checkTerms() uint64 {
	c.t.Helper()
	for iters := 0; iters < 15; iters++ {
		time.Sleep(50 * time.Millisecond)
		seen := map[uint64]struct{}{}
		for i := 0; i < c.n; i++ {
			if !c.connected[i] {
				continue
			}
			seen[c.term(i)] = struct{}{}
		}
		if len(seen) == 1 {
			for t := range seen {
				return t
			}
		}
	}
	c.t.Fatalf("connected nodes never agreed on a single term")
	return 0
}
