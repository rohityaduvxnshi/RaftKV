// Package bolt is a bbolt-backed raft.Persister: the durable, WAL-style store for
// hard state (currentTerm + votedFor), the log, and the snapshot. bbolt fsyncs on
// every transaction commit by default, so each Save*/Append*/Truncate* call is on
// the durability critical path — exactly what Raft needs before it may respond to
// an RPC whose safety depends on the write surviving a crash.
package bolt

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"time"

	bbolt "go.etcd.io/bbolt"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
)

var (
	metaBucket = []byte("meta")
	logBucket  = []byte("log")
	snapBucket = []byte("snap")

	hardStateKey = []byte("hardstate")
	snapshotKey  = []byte("snapshot")
)

// Persister stores Raft state in a single bbolt file. It is safe for concurrent
// use (bbolt serializes writes; reads use a read transaction).
type Persister struct {
	db *bbolt.DB
}

var _ raft.Persister = (*Persister)(nil)

// Open opens (creating if needed) the bbolt file at path and ensures the buckets
// exist. A short open timeout surfaces a stale lock instead of hanging.
func Open(path string) (*Persister, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("bolt: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{metaBucket, logBucket, snapBucket} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Persister{db: db}, nil
}

func (p *Persister) SaveHardState(hs raft.HardState) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(metaBucket).Put(hardStateKey, mustEncode(hs))
	})
}

func (p *Persister) AppendEntries(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return p.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(logBucket)
		for _, e := range entries {
			if err := b.Put(u64key(e.Index), mustEncode(e)); err != nil {
				return err
			}
		}
		return nil
	})
}

// TruncateSuffix deletes every log entry with Index >= index.
func (p *Persister) TruncateSuffix(index uint64) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(logBucket).Cursor()
		for k, _ := c.Seek(u64key(index)); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

// TruncatePrefix deletes every log entry with Index < index (post-snapshot compaction).
func (p *Persister) TruncatePrefix(index uint64) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(logBucket).Cursor()
		for k, _ := c.First(); k != nil && binary.BigEndian.Uint64(k) < index; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *Persister) SaveSnapshot(snap raft.Snapshot) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(snapBucket).Put(snapshotKey, mustEncode(snap))
	})
}

// Load reconstructs everything persisted, with log entries in ascending index
// order (bbolt keys are big-endian indices).
func (p *Persister) Load() (raft.PersistentState, error) {
	ps := raft.PersistentState{HardState: raft.HardState{VotedFor: raft.NoVote}}
	err := p.db.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(metaBucket).Get(hardStateKey); v != nil {
			var hs raft.HardState
			if err := decode(v, &hs); err != nil {
				return err
			}
			ps.HardState = hs
			ps.HasState = true
		}
		c := tx.Bucket(logBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e raft.LogEntry
			if err := decode(v, &e); err != nil {
				return err
			}
			ps.Entries = append(ps.Entries, e)
		}
		if v := tx.Bucket(snapBucket).Get(snapshotKey); v != nil {
			var snap raft.Snapshot
			if err := decode(v, &snap); err != nil {
				return err
			}
			ps.Snapshot = snap
			ps.HasSnap = true
		}
		return nil
	})
	return ps, err
}

// LogBytes approximates the on-disk size of the log for compaction decisions.
func (p *Persister) LogBytes() uint64 {
	var n uint64
	_ = p.db.View(func(tx *bbolt.Tx) error {
		n = uint64(tx.Bucket(logBucket).Stats().LeafInuse)
		return nil
	})
	return n
}

func (p *Persister) Close() error { return p.db.Close() }

// --- helpers ---

func u64key(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}

func mustEncode(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic("bolt: encode: " + err.Error())
	}
	return buf.Bytes()
}

func decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
