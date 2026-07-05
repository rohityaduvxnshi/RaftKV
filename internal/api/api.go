// Package api is the client-facing KV service: it consumes Raft's committed
// entries into a kv.Store and answers client requests. Writes go through the
// Raft log (with per-client sessions for exactly-once retries); reads are
// linearized with ReadIndex (never served from stale local state). A request to
// a non-leader is rejected with a hint to the current leader for redirection.
package api

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
)

// ErrNotLeader is returned when this node cannot serve the request because it is
// not the leader (or cannot confirm leadership). The caller should retry against
// the node named by Leader().
var ErrNotLeader = errors.New("api: not leader")

// ErrTimeout is returned when a request's context expires before its log entry
// commits (or an applied read index is reached).
var ErrTimeout = errors.New("api: timed out")

// raftNode is the slice of Raft the API depends on (satisfied by *raft.Raft;
// narrow interface keeps the API testable and the coupling explicit).
type raftNode interface {
	Submit(command []byte) (index, term uint64, isLeader bool)
	ReadIndex(ctx context.Context) (index uint64, ok bool)
	LeaderID() int
}

// Server wraps one Raft node and its state machine.
type Server struct {
	id    int
	rf    raftNode
	store *kv.Store

	mu           sync.Mutex
	appliedIndex uint64
	waiters      map[uint64]chan applyResult // log index -> waiter

	dead chan struct{}
	wg   sync.WaitGroup
}

type applyResult struct {
	result kv.Result
	term   uint64
}

// NewServer starts consuming applyCh and returns a ready server. Call Close to
// stop it (after the underlying Raft node is killed, so nothing else writes to
// applyCh).
func NewServer(id int, rf raftNode, store *kv.Store, applyCh chan raft.ApplyMsg) *Server {
	s := &Server{
		id:      id,
		rf:      rf,
		store:   store,
		waiters: make(map[uint64]chan applyResult),
		dead:    make(chan struct{}),
	}
	s.wg.Add(1)
	go s.applyLoop(applyCh)
	return s
}

// Close stops the apply loop.
func (s *Server) Close() {
	close(s.dead)
	s.wg.Wait()
}

// Leader returns the node ID the underlying Raft last recognized as leader, for
// redirecting a misdirected request. -1 if unknown.
func (s *Server) Leader() int { return s.rf.LeaderID() }

// applyLoop applies committed commands to the state machine (kv dedups retries),
// installs snapshots, and wakes any client waiting on a committed index.
func (s *Server) applyLoop(applyCh chan raft.ApplyMsg) {
	defer s.wg.Done()
	for {
		select {
		case <-s.dead:
			return
		case msg := <-applyCh:
			if msg.SnapshotValid {
				s.store.Restore(msg.Snapshot)
				s.mu.Lock()
				if msg.SnapshotIndex > s.appliedIndex {
					s.appliedIndex = msg.SnapshotIndex
				}
				s.mu.Unlock()
				continue
			}
			var res kv.Result
			if !msg.NoOp {
				res = s.store.Apply(msg.Command)
			}
			s.mu.Lock()
			s.appliedIndex = msg.Index
			if ch, ok := s.waiters[msg.Index]; ok {
				delete(s.waiters, msg.Index)
				ch <- applyResult{result: res, term: msg.Term} // buffered; never blocks
			}
			s.mu.Unlock()
		}
	}
}

// mutate submits cmd to Raft and waits for it to commit and apply, returning its
// result. If the entry that commits at our index carries a different term, a new
// leader overwrote our proposal — we report ErrNotLeader so the client retries
// (exactly-once dedup makes the retry safe).
func (s *Server) mutate(ctx context.Context, cmd []byte) (kv.Result, error) {
	// Register the waiter under s.mu spanning Submit. applyLoop must take s.mu to
	// deliver a result, so it cannot notify-and-drop our index before we have
	// registered — closing a lost-wakeup window that a fast commit (e.g. a
	// single-node cluster, where Submit advances commitIndex synchronously) would
	// otherwise expose as a spurious timeout. No deadlock: Submit never waits on
	// s.mu and applyCh is buffered, so the applier feeding it can't block Submit.
	s.mu.Lock()
	index, term, isLeader := s.rf.Submit(cmd)
	if !isLeader {
		s.mu.Unlock()
		return kv.Result{}, ErrNotLeader
	}
	ch := make(chan applyResult, 1)
	s.waiters[index] = ch
	s.mu.Unlock()

	select {
	case a := <-ch:
		if a.term != term {
			return kv.Result{}, ErrNotLeader // our entry was overwritten
		}
		return a.result, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return kv.Result{}, ErrTimeout
	case <-s.dead:
		return kv.Result{}, ErrTimeout
	}
}

// Put/Append/Delete/CAS submit a mutating command carrying the client's session
// (clientID + seqNo) so a retried request is applied exactly once.
func (s *Server) Put(ctx context.Context, clientID string, seq uint64, key, value string) error {
	_, err := s.mutate(ctx, kv.Encode(kv.Op{Type: kv.OpPut, Key: key, Value: value, ClientID: clientID, SeqNo: seq}))
	return err
}

func (s *Server) Append(ctx context.Context, clientID string, seq uint64, key, value string) (string, error) {
	r, err := s.mutate(ctx, kv.Encode(kv.Op{Type: kv.OpAppend, Key: key, Value: value, ClientID: clientID, SeqNo: seq}))
	return r.Value, err
}

func (s *Server) Delete(ctx context.Context, clientID string, seq uint64, key string) (bool, error) {
	r, err := s.mutate(ctx, kv.Encode(kv.Op{Type: kv.OpDelete, Key: key, ClientID: clientID, SeqNo: seq}))
	return r.Found, err
}

func (s *Server) CAS(ctx context.Context, clientID string, seq uint64, key, expected, value string) (bool, error) {
	r, err := s.mutate(ctx, kv.Encode(kv.Op{Type: kv.OpCAS, Key: key, Expected: expected, Value: value, ClientID: clientID, SeqNo: seq}))
	return r.Found, err
}

// Get performs a linearizable read: it obtains a ReadIndex (which confirms this
// node is still the leader), waits until the state machine has applied through
// that index, then reads. It never returns a value older than the latest write
// acknowledged before the read began.
func (s *Server) Get(ctx context.Context, key string) (value string, found bool, err error) {
	readIndex, ok := s.rf.ReadIndex(ctx)
	if !ok {
		return "", false, ErrNotLeader
	}
	if !s.waitApplied(ctx, readIndex) {
		return "", false, ErrTimeout
	}
	v, f := s.store.Get(key)
	return v, f, nil
}

// waitApplied blocks until the state machine has applied through index, or the
// context/shutdown fires. Reads apply promptly, so the poll interval is small.
func (s *Server) waitApplied(ctx context.Context, index uint64) bool {
	for {
		s.mu.Lock()
		done := s.appliedIndex >= index
		s.mu.Unlock()
		if done {
			return true
		}
		select {
		case <-time.After(2 * time.Millisecond):
		case <-ctx.Done():
			return false
		case <-s.dead:
			return false
		}
	}
}
