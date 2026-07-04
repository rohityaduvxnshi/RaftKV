// Package inmem is a simulated in-process Transport for deterministic tests.
//
// It routes Raft RPCs between registered nodes and can inject faults — message
// drop, delay/reorder, and network partition — driven by a seeded RNG so a
// failing scenario reproduces from its seed. Two honesty caveats: (1) faults are
// reproducible in their *decisions* (which messages drop, how long they delay),
// but goroutine scheduling is not bit-for-bit deterministic, so tests assert
// properties over time windows rather than exact interleavings; (2) every RPC's
// args and reply are gob-cloned on the way through, modelling wire serialization
// and guaranteeing sender and receiver never share memory (catches accidental
// aliasing and keeps the race detector honest).
//
// The Raft core is oblivious to all of this: it only ever sees raft.Transport.
package inmem

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
)

// Network routes Raft RPCs between registered nodes, keyed by node ID.
type Network struct {
	mu        sync.Mutex
	handlers  map[int]raft.RPCHandler
	connected map[int]bool
	rng       *rand.Rand
	reliable  bool // when false, inject drops + delays
}

// NewNetwork returns a reliable network (no drops/delays) seeded for any later
// fault injection.
func NewNetwork(seed int64) *Network {
	return &Network{
		handlers:  make(map[int]raft.RPCHandler),
		connected: make(map[int]bool),
		rng:       rand.New(rand.NewSource(seed)),
		reliable:  true,
	}
}

// SetReliable toggles fault injection. Reliable = perfect delivery; unreliable =
// ~10% drop and 0–27 ms random delay per RPC (which also reorders concurrent
// messages).
func (n *Network) SetReliable(reliable bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = reliable
}

// Register attaches a node's inbound handler and marks it connected. Re-registering
// the same ID (e.g. after a simulated restart) replaces the handler.
func (n *Network) Register(id int, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.connected[id] = true
}

// SetConnected partitions (false) or heals (true) a node: while disconnected,
// every RPC to or from it is dropped, modelling a network partition. The node
// keeps running — it just can't talk to anyone.
func (n *Network) SetConnected(id int, connected bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = connected
}

// Transport returns the raft.Transport node `from` uses to reach peers.
func (n *Network) Transport(from int) raft.Transport {
	return &transport{net: n, from: from}
}

// route decides the fate of one RPC from->to under the current fault config,
// returning the destination handler and any delay to apply. ok=false means the
// message is lost (partition, node down, or random drop) — the caller surfaces
// raft.ErrUnreachable. The RNG is advanced here, under the lock, so a given seed
// drives a reproducible sequence of drop/delay decisions.
func (n *Network) route(from, to int) (h raft.RPCHandler, delay time.Duration, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dst := n.handlers[to]
	if dst == nil || !n.connected[from] || !n.connected[to] {
		return nil, 0, false
	}
	if n.reliable {
		return dst, 0, true
	}
	if n.rng.Intn(100) < 10 { // ~10% drop
		return nil, 0, false
	}
	return dst, time.Duration(n.rng.Intn(27)) * time.Millisecond, true
}

type transport struct {
	net  *Network
	from int
}

var _ raft.Transport = (*transport)(nil)

func (t *transport) SendRequestVote(ctx context.Context, peer int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, delay, ok := t.net.route(t.from, peer)
	if !ok {
		return nil, raft.ErrUnreachable
	}
	if !sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	a := gobClone(*args)
	reply := h.HandleRequestVote(&a)
	r := gobClone(*reply)
	return &r, nil
}

func (t *transport) SendAppendEntries(ctx context.Context, peer int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, delay, ok := t.net.route(t.from, peer)
	if !ok {
		return nil, raft.ErrUnreachable
	}
	if !sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	a := gobClone(*args)
	reply := h.HandleAppendEntries(&a)
	r := gobClone(*reply)
	return &r, nil
}

func (t *transport) SendInstallSnapshot(ctx context.Context, peer int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	h, delay, ok := t.net.route(t.from, peer)
	if !ok {
		return nil, raft.ErrUnreachable
	}
	if !sleep(ctx, delay) {
		return nil, ctx.Err()
	}
	a := gobClone(*args)
	reply := h.HandleInstallSnapshot(&a)
	r := gobClone(*reply)
	return &r, nil
}

// sleep waits for d, returning false if the context is cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// gobClone deep-copies v via gob, modelling serialization across the wire so the
// receiver can never observe or mutate the sender's memory.
func gobClone[T any](v T) T {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(err) // args/reply types are always gob-encodable
	}
	var out T
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		panic(err)
	}
	return out
}
