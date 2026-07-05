// Package kv is the replicated key-value state machine that sits behind Raft.
// Raft treats commands as opaque bytes; this package defines their encoding and
// applies them deterministically. Applying the same committed log on every node
// yields identical state (State Machine Safety).
//
// It also implements exactly-once semantics for client retries: each mutating
// command carries a (ClientID, SeqNo); the state machine remembers the last
// SeqNo applied per client (and its result), so a command that is committed
// twice — e.g. because a leader crashed after committing but before replying and
// the client retried — mutates the state only once.
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
)

// OpType enumerates the operations. Append is non-idempotent (it accumulates), so
// it makes duplicate application observable in state — the exactly-once test
// relies on that.
type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpDelete
	OpCAS
	OpAppend
)

// Op is a single command, gob-encoded into a Raft log entry's Command. ClientID
// and SeqNo are empty/zero for commands that don't need dedup (e.g. internal or
// test writes).
type Op struct {
	Type     OpType
	Key      string
	Value    string
	Expected string // OpCAS: only swap if the current value equals this
	ClientID string
	SeqNo    uint64
}

// Result is what applying an Op produces, returned to a waiting client.
type Result struct {
	Value string // OpGet/OpAppend: the resulting value
	Found bool   // OpGet: key existed; OpCAS: swap happened; OpDelete: key existed
}

type session struct {
	LastSeq    uint64
	LastResult Result
}

// Store is the key-value state machine plus per-client session state. Safe for
// concurrent use.
type Store struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session
}

// New returns an empty store.
func New() *Store {
	return &Store{data: make(map[string]string), sessions: make(map[string]session)}
}

// Apply executes one committed command and returns its result, deduplicating
// retries: if the command carries a client session and its SeqNo was already
// applied, the state is left untouched and the cached result is returned.
func (s *Store) Apply(cmd []byte) Result {
	op := decode(cmd)
	s.mu.Lock()
	defer s.mu.Unlock()

	// A non-empty ClientID signals dedup intent; SeqNo 0 is a valid first sequence
	// number (don't silently skip dedup for a client that 0-indexes its requests).
	tracked := op.ClientID != ""
	if tracked {
		if sess, ok := s.sessions[op.ClientID]; ok && op.SeqNo <= sess.LastSeq {
			return sess.LastResult // duplicate: don't re-apply
		}
	}
	res := s.applyOp(op)
	if tracked {
		s.sessions[op.ClientID] = session{LastSeq: op.SeqNo, LastResult: res}
	}
	return res
}

// applyOp performs the mutation. Caller holds the write lock.
func (s *Store) applyOp(op Op) Result {
	switch op.Type {
	case OpPut:
		s.data[op.Key] = op.Value
		return Result{}
	case OpAppend:
		s.data[op.Key] += op.Value
		return Result{Value: s.data[op.Key]}
	case OpDelete:
		_, ok := s.data[op.Key]
		delete(s.data, op.Key)
		return Result{Found: ok}
	case OpCAS:
		cur, ok := s.data[op.Key]
		if ok && cur == op.Expected {
			s.data[op.Key] = op.Value
			return Result{Found: true}
		}
		return Result{Found: false}
	case OpGet:
		v, ok := s.data[op.Key]
		return Result{Value: v, Found: ok}
	default:
		return Result{}
	}
}

// Get reads the current value directly. Linearizable reads go through the API's
// ReadIndex path, not this method.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Dump returns a copy of the store's key-value contents (tests compare state
// across nodes order-independently).
func (s *Store) Dump() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[string]string, len(s.data))
	for k, v := range s.data {
		m[k] = v
	}
	return m
}

// snapshotState is the serialized form of the whole state machine — data AND the
// session table, so exactly-once dedup survives log compaction.
type snapshotState struct {
	Data     map[string]string
	Sessions map[string]session
}

// Snapshot serializes the whole store (data + sessions) for Raft log compaction.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotState{Data: s.data, Sessions: s.sessions}); err != nil {
		panic("kv: snapshot: " + err.Error())
	}
	return buf.Bytes()
}

// Restore replaces the store's contents from a snapshot produced by Snapshot.
func (s *Store) Restore(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := snapshotState{Data: map[string]string{}, Sessions: map[string]session{}}
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
			panic("kv: restore: " + err.Error())
		}
	}
	if st.Data == nil {
		st.Data = map[string]string{}
	}
	if st.Sessions == nil {
		st.Sessions = map[string]session{}
	}
	s.data = st.Data
	s.sessions = st.Sessions
}

// --- command encoding ---

// Encode serializes an Op (used by the API to build session-carrying commands).
func Encode(op Op) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		panic("kv: encode: " + err.Error())
	}
	return buf.Bytes()
}

func decode(cmd []byte) Op {
	var op Op
	if err := gob.NewDecoder(bytes.NewReader(cmd)).Decode(&op); err != nil {
		panic("kv: decode: " + err.Error())
	}
	return op
}

// Convenience builders for session-less commands (used by the replication tests).
func EncodePut(key, value string) []byte { return Encode(Op{Type: OpPut, Key: key, Value: value}) }
func EncodeAppend(key, value string) []byte {
	return Encode(Op{Type: OpAppend, Key: key, Value: value})
}
func EncodeDelete(key string) []byte { return Encode(Op{Type: OpDelete, Key: key}) }
func EncodeGet(key string) []byte    { return Encode(Op{Type: OpGet, Key: key}) }
func EncodeCAS(key, expected, value string) []byte {
	return Encode(Op{Type: OpCAS, Key: key, Expected: expected, Value: value})
}
