package raft

// PersistentState is everything reloaded from stable storage when a node
// restarts. Entries holds the log suffix that survives after Snapshot (i.e.
// entries with Index > Snapshot.LastIncludedIndex).
type PersistentState struct {
	HardState HardState
	HasState  bool // false on a brand-new node with nothing persisted yet

	Entries []LogEntry

	Snapshot Snapshot
	HasSnap  bool
}

// Persister is Raft's durable store for hard state, the log, and the snapshot.
//
// The interface is intentionally shaped around Raft's *incremental* access
// pattern rather than "dump the whole state" — a real write-ahead log appends
// one entry at a time and must not re-serialize the entire log on every commit.
// The durability contract: a method that persists MUST NOT return until the
// data is safely on stable storage (fsync'd) — Raft calls these on the critical
// path before responding to RPCs whose safety depends on them.
//
// Two implementations: an in-memory Persister (tests) and a bbolt-backed one
// (deployment, Phase 3).
type Persister interface {
	// SaveHardState durably records currentTerm + votedFor.
	SaveHardState(hs HardState) error

	// AppendEntries durably appends entries to the end of the log. Callers
	// guarantee entries are contiguous and follow the current last index.
	AppendEntries(entries []LogEntry) error

	// TruncateSuffix removes every entry with Index >= index. Used when a
	// follower overwrites conflicting uncommitted entries from a stale leader.
	TruncateSuffix(index uint64) error

	// TruncatePrefix removes every entry with Index < index. Used after a
	// snapshot compacts the log prefix.
	TruncatePrefix(index uint64) error

	// SaveSnapshot durably stores the snapshot (atomically w.r.t. crashes).
	SaveSnapshot(snap Snapshot) error

	// Load returns all persisted state at startup.
	Load() (PersistentState, error)

	// LogBytes reports the approximate on-disk size of the persisted log,
	// used to decide when to snapshot/compact.
	LogBytes() uint64

	// Close releases resources (flushes and closes the underlying store).
	Close() error
}
