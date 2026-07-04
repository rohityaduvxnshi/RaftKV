package test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	"github.com/rohityaduvxnshi/RaftKV/internal/storage/bolt"
	"github.com/rohityaduvxnshi/RaftKV/internal/transport/inmem"
)

// cluster is an N-node Raft test cluster on the simulated in-mem network.
// GetState/connect/disconnect are called only from the test goroutine; the
// apply-tracking state (guarded by amu) is written by per-node drain goroutines.
type cluster struct {
	t          *testing.T
	n          int
	seed       int64
	peers      []int
	net        *inmem.Network
	rafts      []*raft.Raft
	persisters []raft.Persister
	pfactory   func(i int) raft.Persister // returns node i's durable store (same across restarts)
	applyChs   []chan raft.ApplyMsg
	connected  []bool

	amu       sync.Mutex
	committed map[uint64][]byte   // index -> the agreed command (State Machine Safety oracle)
	applied   []map[uint64][]byte // per node: index -> command it applied
	nextApply []uint64            // per node: next index expected (in-order check)
	done      chan struct{}
}

// makeCluster builds and starts an N-node cluster backed by in-memory
// persisters. Pass reliable=false to inject message drops and delays. Everything
// is reproducible from seed.
func makeCluster(t *testing.T, n int, seed int64, reliable bool) *cluster {
	mems := make([]*raft.MemPersister, n)
	factory := func(i int) raft.Persister {
		if mems[i] == nil {
			mems[i] = raft.NewMemPersister()
		}
		return mems[i] // same object across restarts: state survives
	}
	return newCluster(t, n, seed, reliable, factory)
}

// makeClusterBolt builds an N-node cluster backed by real bbolt files under a
// temp dir, so crashAndRestart genuinely reloads durable state from disk.
func makeClusterBolt(t *testing.T, n int, seed int64, reliable bool) *cluster {
	dir := t.TempDir()
	factory := func(i int) raft.Persister {
		p, err := bolt.Open(filepath.Join(dir, fmt.Sprintf("raft-%d.db", i)))
		if err != nil {
			t.Fatalf("open bolt for node %d: %v", i, err)
		}
		return p // reopening the same file reloads on-disk state
	}
	return newCluster(t, n, seed, reliable, factory)
}

func newCluster(t *testing.T, n int, seed int64, reliable bool, pfactory func(i int) raft.Persister) *cluster {
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	c := &cluster{
		t:          t,
		n:          n,
		seed:       seed,
		peers:      peers,
		net:        inmem.NewNetwork(seed),
		rafts:      make([]*raft.Raft, n),
		persisters: make([]raft.Persister, n),
		pfactory:   pfactory,
		applyChs:   make([]chan raft.ApplyMsg, n),
		connected:  make([]bool, n),
		committed:  make(map[uint64][]byte),
		applied:    make([]map[uint64][]byte, n),
		nextApply:  make([]uint64, n),
		done:       make(chan struct{}),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = pfactory(i)
		c.applyChs[i] = make(chan raft.ApplyMsg, 256)
		c.applied[i] = make(map[uint64][]byte)
		c.nextApply[i] = 1 // index 0 is the sentinel, never applied
		c.rafts[i] = raft.New(raft.Config{
			ID:        i,
			Peers:     peers,
			Transport: c.net.Transport(i),
			Persister: c.persisters[i],
			ApplyCh:   c.applyChs[i],
			Seed:      seed,
		})
		c.net.Register(i, c.rafts[i])
		c.connected[i] = true
	}
	for i := 0; i < n; i++ {
		go c.drain(i, c.applyChs[i])
		c.rafts[i].Start()
	}
	return c
}

// bringUp (re)creates node i from its (reopened) persister, restoring its
// connection state and a fresh apply drain, but does NOT start it. The node
// replays its committed log from index 1, so its apply tracking is reset.
func (c *cluster) bringUp(i int) {
	c.persisters[i] = c.pfactory(i)
	c.applyChs[i] = make(chan raft.ApplyMsg, 256)
	c.amu.Lock()
	c.nextApply[i] = 1
	c.applied[i] = make(map[uint64][]byte)
	c.amu.Unlock()
	c.rafts[i] = raft.New(raft.Config{
		ID:        i,
		Peers:     c.peers,
		Transport: c.net.Transport(i),
		Persister: c.persisters[i],
		ApplyCh:   c.applyChs[i],
		Seed:      c.seed,
	})
	c.net.Register(i, c.rafts[i])
	c.net.SetConnected(i, c.connected[i]) // Register re-connects; restore partition state
	go c.drain(i, c.applyChs[i])
}

// crashAndRestart simulates a kill -9 + restart of node i: stop it, close and
// reopen its persister (reloading durable state from disk for bbolt), and start
// a fresh Raft that must recover its log and rejoin.
func (c *cluster) crashAndRestart(i int) {
	c.rafts[i].Kill()
	_ = c.persisters[i].Close()
	c.bringUp(i)
	c.rafts[i].Start()
}

// crashAllAndRestart takes the WHOLE cluster down at once, then brings it back —
// the strongest durability test: nothing is alive to serve the recovered nodes,
// so committed data must come entirely from disk.
func (c *cluster) crashAllAndRestart() {
	for i := 0; i < c.n; i++ {
		c.rafts[i].Kill()
		_ = c.persisters[i].Close()
	}
	for i := 0; i < c.n; i++ {
		c.bringUp(i)
	}
	for i := 0; i < c.n; i++ {
		c.rafts[i].Start()
	}
}

func (c *cluster) cleanup() {
	for _, r := range c.rafts {
		r.Kill() // stops each applier; safe to do before closing done
	}
	close(c.done)
	for _, p := range c.persisters {
		_ = p.Close()
	}
}

// drain consumes a node's committed entries, asserting State Machine Safety (all
// nodes apply the same command at each index) and in-order, gap-free apply.
func (c *cluster) drain(i int, ch chan raft.ApplyMsg) {
	for {
		select {
		case msg := <-ch:
			c.amu.Lock()
			if prev, ok := c.committed[msg.Index]; ok {
				if !bytes.Equal(prev, msg.Command) {
					c.amu.Unlock()
					c.t.Errorf("State Machine Safety violated at index %d: node %d applied %q, expected %q",
						msg.Index, i, msg.Command, prev)
					continue
				}
			} else {
				c.committed[msg.Index] = append([]byte(nil), msg.Command...)
			}
			if msg.Index != c.nextApply[i] {
				c.t.Errorf("node %d applied index %d out of order (expected %d)", i, msg.Index, c.nextApply[i])
			}
			c.nextApply[i] = msg.Index + 1
			c.applied[i][msg.Index] = append([]byte(nil), msg.Command...)
			c.amu.Unlock()
		case <-c.done:
			return
		}
	}
}

// nCommitted reports how many nodes have applied the entry at index, plus the
// agreed command there.
func (c *cluster) nCommitted(index uint64) (int, []byte) {
	c.amu.Lock()
	defer c.amu.Unlock()
	count := 0
	for i := 0; i < c.n; i++ {
		if _, ok := c.applied[i][index]; ok {
			count++
		}
	}
	return count, c.committed[index]
}

// one submits cmd through the current leader and waits until at least
// expectedServers nodes have committed it at the same index, returning that
// index. It retries through leader changes (resubmitting on a different node).
// It is safe to call from worker goroutines: on failure it records the error
// with Errorf (not Fatalf) and returns 0, per the testing contract that FailNow
// run only on the test goroutine.
func (c *cluster) one(cmd []byte, expectedServers int, retry bool) uint64 {
	c.t.Helper()
	t0 := time.Now()
	for time.Since(t0) < 5*time.Second {
		index, ok := uint64(0), false
		for i := 0; i < c.n; i++ {
			if !c.connected[i] {
				continue
			}
			if idx, _, isLeader := c.rafts[i].Submit(cmd); isLeader {
				index, ok = idx, true
				break
			}
		}
		if ok {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if cnt, actual := c.nCommitted(index); cnt >= expectedServers && bytes.Equal(actual, cmd) {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Errorf("one(%q): not committed by %d servers", cmd, expectedServers)
				return 0
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Errorf("one(%q): no agreement within 5s", cmd)
	return 0
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
