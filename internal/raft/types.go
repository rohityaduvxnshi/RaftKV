// Package raft implements the Raft consensus protocol (Ongaro & Ousterhout)
// from scratch: leader election, log replication, persistence, snapshotting,
// and linearizable reads. The core depends only on the Transport and Persister
// interfaces defined in this package, so the same code runs against an
// in-memory simulated network (deterministic, adversarial tests) and a real
// gRPC network (deployment).
//
// Indexing convention: log indices are 1-based. Index 0 is the zero entry that
// precedes the first real entry (or the snapshot boundary). Peer/node IDs are
// dense ints in [0, N).
package raft

// NoVote is the sentinel VotedFor value meaning "have not voted this term".
const NoVote = -1

// LogEntry is a single command replicated through the Raft log. Command is an
// opaque blob interpreted only by the state machine (the KV store), never by
// Raft itself.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// HardState is the subset of Raft state that MUST survive a crash before the
// node responds to any RPC that depends on it (Figure 2, "Persistent state").
type HardState struct {
	CurrentTerm uint64
	VotedFor    int // node ID voted for in CurrentTerm, or NoVote
}

// Snapshot captures state-machine state up to and including LastIncludedIndex,
// allowing the log prefix at or before that index to be discarded.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// --- RPC message types (Raft Figure 2) ---

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast conflict backup (Ongaro §5.3 optimization): when Success is false
	// because of a log conflict, the follower reports the term of the
	// conflicting entry and the first index it holds for that term so the
	// leader can skip a whole term's worth of entries in one round trip.
	ConflictTerm  uint64
	ConflictIndex uint64
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}
