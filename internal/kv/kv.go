// Package kv is the replicated key-value state machine that sits behind Raft.
// Raft treats commands as opaque bytes; this package defines their encoding and
// applies them deterministically. Applying the same committed log on every node
// yields identical state (State Machine Safety).
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
)

// OpType enumerates the mutating operations plus Get (which the state machine
// can serve, though linearizable reads are wired up in Phase 5).
type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpDelete
	OpCAS
)

// Op is a single command. It is gob-encoded into a Raft log entry's Command.
type Op struct {
	Type     OpType
	Key      string
	Value    string
	Expected string // OpCAS: only swap if the current value equals this
	// Phase 5 adds ClientID + SeqNo here for exactly-once semantics.
}

// Result is what applying an Op produces, returned to a waiting client.
type Result struct {
	Value string // OpGet: the current value
	Found bool   // OpGet: key existed; OpCAS: swap happened; OpDelete: key existed
}

// Store is the key-value state machine. Safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply executes one committed command against the state machine and returns its
// result. Applying commands in log order on every node keeps the stores in sync.
func (s *Store) Apply(cmd []byte) Result {
	op := decode(cmd)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op.Type {
	case OpPut:
		s.data[op.Key] = op.Value
		return Result{}
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

// Get reads the current value directly (a local, not-yet-linearizable read).
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Dump returns a copy of the store's contents (for tests: compare state across
// nodes order-independently, since gob map bytes aren't stable across encoders).
func (s *Store) Dump() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[string]string, len(s.data))
	for k, v := range s.data {
		m[k] = v
	}
	return m
}

// Snapshot serializes the whole store, for Raft log compaction.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.data); err != nil {
		panic("kv: snapshot: " + err.Error())
	}
	return buf.Bytes()
}

// Restore replaces the store's contents from a snapshot produced by Snapshot.
func (s *Store) Restore(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[string]string)
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
			panic("kv: restore: " + err.Error())
		}
	}
	s.data = m
}

// --- command encoding ---

func encode(op Op) []byte {
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

// EncodePut / EncodeDelete / EncodeCAS build commands to submit through Raft.
func EncodePut(key, value string) []byte { return encode(Op{Type: OpPut, Key: key, Value: value}) }
func EncodeDelete(key string) []byte     { return encode(Op{Type: OpDelete, Key: key}) }
func EncodeGet(key string) []byte        { return encode(Op{Type: OpGet, Key: key}) }
func EncodeCAS(key, expected, value string) []byte {
	return encode(Op{Type: OpCAS, Key: key, Expected: expected, Value: value})
}
