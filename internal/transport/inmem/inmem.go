// Package inmem is a simulated in-process Transport for deterministic tests.
//
// Phase 0: reliable, synchronous delivery — just enough to exercise the
// Transport/RPCHandler plumbing under -race. Later phases extend Network with a
// seeded fault model (drop, reorder, delay, partition) so adversarial failures
// reproduce exactly under a fixed seed. The Raft core is oblivious: it only ever
// sees the raft.Transport interface.
package inmem

import (
	"context"
	"sync"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
)

// Network routes Raft RPCs between registered nodes, keyed by node ID.
type Network struct {
	mu       sync.RWMutex
	handlers map[int]raft.RPCHandler
}

// NewNetwork returns an empty network with no nodes attached.
func NewNetwork() *Network {
	return &Network{handlers: make(map[int]raft.RPCHandler)}
}

// Register attaches a node's inbound RPC handler under its ID. Re-registering
// the same ID (e.g. after a simulated crash/restart) replaces the handler.
func (n *Network) Register(id int, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Deregister detaches a node (simulating it going down). Sends to it then
// return ErrUnreachable.
func (n *Network) Deregister(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

func (n *Network) handler(id int) raft.RPCHandler {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.handlers[id]
}

// Transport returns the raft.Transport that node `from` uses to reach peers.
func (n *Network) Transport(from int) raft.Transport {
	return &transport{net: n, from: from}
}

// transport is one node's view of the network. `from` identifies the sender;
// later phases use it to enforce partitions (a partitioned pair can't talk).
type transport struct {
	net  *Network
	from int
}

var _ raft.Transport = (*transport)(nil)

func (t *transport) SendRequestVote(ctx context.Context, peer int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h := t.net.handler(peer)
	if h == nil {
		return nil, raft.ErrUnreachable
	}
	return h.HandleRequestVote(args), nil
}

func (t *transport) SendAppendEntries(ctx context.Context, peer int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h := t.net.handler(peer)
	if h == nil {
		return nil, raft.ErrUnreachable
	}
	return h.HandleAppendEntries(args), nil
}

func (t *transport) SendInstallSnapshot(ctx context.Context, peer int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	h := t.net.handler(peer)
	if h == nil {
		return nil, raft.ErrUnreachable
	}
	return h.HandleInstallSnapshot(args), nil
}
