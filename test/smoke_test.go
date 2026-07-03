package test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	"github.com/rohityaduvxnshi/RaftKV/internal/transport/inmem"
)

// echoHandler is a stand-in RPCHandler for Phase 0: it acknowledges every RPC by
// echoing the caller's term. The real handler (with election/replication logic)
// arrives in Phase 1. This exists only to smoke-test the transport plumbing.
type echoHandler struct {
	mu    sync.Mutex
	calls int
}

func (h *echoHandler) HandleRequestVote(a *raft.RequestVoteArgs) *raft.RequestVoteReply {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: true}
}

func (h *echoHandler) HandleAppendEntries(a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return &raft.AppendEntriesReply{Term: a.Term, Success: true}
}

func (h *echoHandler) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return &raft.InstallSnapshotReply{Term: a.Term}
}

// TestInmemRoundTrip verifies a registered peer answers, an unregistered peer is
// unreachable, and concurrent sends are race-free (this test must pass -race).
func TestInmemRoundTrip(t *testing.T) {
	net := inmem.NewNetwork()
	h := &echoHandler{}
	net.Register(1, h)
	tr := net.Transport(0)
	ctx := context.Background()

	// Concurrent sends from node 0 to node 1 exercise the network's locking.
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(term uint64) {
			defer wg.Done()
			reply, err := tr.SendRequestVote(ctx, 1, &raft.RequestVoteArgs{Term: term, CandidateID: 0})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !reply.VoteGranted || reply.Term != term {
				t.Errorf("bad reply: %+v (want term %d)", reply, term)
			}
		}(uint64(i))
	}
	wg.Wait()

	if h.calls != n {
		t.Fatalf("handler saw %d calls, want %d", h.calls, n)
	}

	// An unregistered peer must report ErrUnreachable, not panic.
	if _, err := tr.SendAppendEntries(ctx, 99, &raft.AppendEntriesArgs{}); !errors.Is(err, raft.ErrUnreachable) {
		t.Fatalf("send to unknown peer: got %v, want ErrUnreachable", err)
	}
}
