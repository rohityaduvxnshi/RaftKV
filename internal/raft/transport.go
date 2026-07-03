package raft

import (
	"context"
	"errors"
)

// ErrUnreachable is returned by a Transport when the target peer cannot be
// reached (partitioned, down, or dropped by the simulated network). It is a
// normal, expected condition — Raft treats it like any other failed RPC and
// retries later.
var ErrUnreachable = errors.New("raft: peer unreachable")

// Transport sends outbound Raft RPCs to a peer identified by its node ID. Each
// method blocks until a reply arrives or the context is cancelled. The Raft
// core MUST call these off its own lock (one goroutine per outbound RPC), since
// a call may block on the network.
//
// Two implementations exist: an in-memory simulated network (tests, supports
// drop/reorder/delay/partition under a seed) and a gRPC transport (deployment).
type Transport interface {
	SendRequestVote(ctx context.Context, peer int, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, peer int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler receives inbound Raft RPCs. It is implemented by *Raft. A Transport
// delivers incoming RPCs by invoking these methods on the destination node.
// Handlers must be safe for concurrent calls.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}
