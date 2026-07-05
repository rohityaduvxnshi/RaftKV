package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Timing. Election timeouts are randomized in [electionTimeoutMin,
// electionTimeoutMax) so nodes don't repeatedly split the vote; the heartbeat
// interval is well under the minimum election timeout so a live leader always
// refreshes followers before they time out. tickInterval is how often the
// background loop wakes to check deadlines.
const (
	tickInterval       = 12 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

// Role is a node's current role in the current term.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ApplyMsg is delivered, in order, to the state machine via the apply channel.
// Exactly one of CommandValid / SnapshotValid is set: a committed command to
// apply, or a snapshot to install (after a follower catches up via
// InstallSnapshot, or on restart-from-snapshot).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	Index        uint64
	Term         uint64
	NoOp         bool // an election barrier entry: advance appliedIndex but don't apply

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Config wires up a Raft node. Peers lists every node ID in the cluster,
// including this node's own ID.
type Config struct {
	ID        int
	Peers     []int
	Transport Transport
	Persister Persister
	ApplyCh   chan ApplyMsg
	Seed      int64
}

type pendingSnap struct {
	index uint64
	term  uint64
	data  []byte
}

// Raft is a single node of the cluster. All mutable state is guarded by mu.
// Outbound RPCs are always sent from goroutines that do NOT hold mu.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int
	transport Transport
	persister Persister

	// Persistent state (Figure 2). The log carries a boundary sentinel at index
	// 0: log[0].Index is the last snapshotted index (0 when no snapshot), and
	// log[i].Index == log[0].Index + i. Absolute index a maps to slice position
	// a - base(). Entries at or below base() live only in the snapshot.
	currentTerm uint64
	votedFor    int
	log         []LogEntry
	snapshot    []byte // bytes of the current snapshot (matches log[0])

	// Volatile state on all servers.
	role        Role
	leaderID    int
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders, indexed by node ID.
	nextIndex  []uint64
	matchIndex []uint64

	electionDeadline time.Time
	rng              *rand.Rand

	applyCh     chan ApplyMsg
	applyCond   *sync.Cond
	pendingSnap *pendingSnap // a snapshot the applier must hand to the state machine

	dead chan struct{}
	wg   sync.WaitGroup
}

// New constructs a node in the Follower role. Register it with the transport,
// then call Start.
func New(cfg Config) *Raft {
	r := &Raft{
		id:          cfg.ID,
		peers:       append([]int(nil), cfg.Peers...),
		transport:   cfg.Transport,
		persister:   cfg.Persister,
		applyCh:     cfg.ApplyCh,
		currentTerm: 0,
		votedFor:    NoVote,
		log:         []LogEntry{{Term: 0, Index: 0}}, // boundary sentinel
		role:        Follower,
		leaderID:    -1,
		rng:         rand.New(rand.NewSource(cfg.Seed + int64(cfg.ID)*2654435761)),
		dead:        make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)

	ps, err := r.persister.Load()
	if err != nil {
		panic("raft: load persisted state: " + err.Error())
	}
	if ps.HasState {
		r.currentTerm = ps.HardState.CurrentTerm
		r.votedFor = ps.HardState.VotedFor
	}
	if ps.HasSnap {
		r.log[0] = LogEntry{Index: ps.Snapshot.LastIncludedIndex, Term: ps.Snapshot.LastIncludedTerm}
		r.snapshot = ps.Snapshot.Data
		r.commitIndex = ps.Snapshot.LastIncludedIndex
		// lastApplied stays 0 so the applier delivers this snapshot to the state
		// machine first (it will then advance lastApplied to LastIncludedIndex);
		// the compacted range is never applied as commands because the pending
		// snapshot is processed before any command.
		r.pendingSnap = &pendingSnap{ps.Snapshot.LastIncludedIndex, ps.Snapshot.LastIncludedTerm, ps.Snapshot.Data}
	}
	// Append persisted entries, but keep only a contiguous suffix starting just
	// past the snapshot boundary. SaveSnapshot and the log truncation are
	// separate transactions, so a crash between them can leave entries the
	// snapshot already covers (Index <= base()) on disk; appending those verbatim
	// would break the log[i].Index == base()+i invariant. Drop them, and stop at
	// any gap rather than reconstruct a non-contiguous log.
	next := r.base() + 1
	for _, e := range ps.Entries {
		if e.Index < next {
			continue // covered by the snapshot
		}
		if e.Index != next {
			break // a gap would corrupt the index invariant
		}
		r.log = append(r.log, e)
		next++
	}
	r.resetElectionTimer()
	return r
}

func (r *Raft) persistAppend(entries []LogEntry) {
	if err := r.persister.AppendEntries(entries); err != nil {
		panic("raft: persist append failed: " + err.Error())
	}
}

func (r *Raft) persistTruncateSuffix(index uint64) {
	if err := r.persister.TruncateSuffix(index); err != nil {
		panic("raft: persist truncate failed: " + err.Error())
	}
}

// Start launches the background loops.
func (r *Raft) Start() {
	r.wg.Add(1)
	go r.run()
	if r.applyCh != nil {
		r.wg.Add(1)
		go r.applier()
	}
}

// Kill stops the node and waits for its goroutines to exit. Call once.
func (r *Raft) Kill() {
	close(r.dead)
	r.mu.Lock()
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Raft) killed() bool {
	select {
	case <-r.dead:
		return true
	default:
		return false
	}
}

// GetState reports the node's current term and whether it believes it is leader.
func (r *Raft) GetState() (term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

// LogSize reports the approximate on-disk size of the (uncompacted) log, so the
// application can decide when to snapshot.
func (r *Raft) LogSize() uint64 { return r.persister.LogBytes() }

// LeaderID returns the ID of the leader this node last recognized, or -1 if
// unknown. The client API uses it to redirect a request to the current leader.
func (r *Raft) LeaderID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// ReadIndex returns a commit index safe to serve a linearizable read at, after
// confirming via a heartbeat quorum that this node is still the leader. ok is
// false if this node is not the leader, has not yet committed an entry in its
// term (the election no-op), or cannot confirm leadership — the caller should
// then redirect to the leader or retry. The caller must also wait until its
// state machine has applied through the returned index before reading.
func (r *Raft) ReadIndex(ctx context.Context) (index uint64, ok bool) {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return 0, false
	}
	term := r.currentTerm
	// §8: only once a current-term entry is committed does commitIndex reflect
	// every committed entry, so a read at it cannot miss an acknowledged write.
	if r.commitIndex <= r.base() || r.entry(r.commitIndex).Term != term {
		r.mu.Unlock()
		return 0, false
	}
	readIndex := r.commitIndex
	r.mu.Unlock()

	if !r.confirmQuorum(ctx, term) {
		return 0, false
	}
	return readIndex, true
}

// confirmQuorum sends one heartbeat round and returns true once a majority
// (including self) still acknowledges this leader for term — proving no newer
// leader has been elected, so a read at the captured index is linearizable.
func (r *Raft) confirmQuorum(ctx context.Context, term uint64) bool {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return false
	}
	prevIndex, prevTerm := r.lastLogInfo()
	leaderCommit := r.commitIndex
	r.mu.Unlock()

	results := make(chan bool, len(r.peers))
	sent := 0
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		sent++
		go func(peer int) {
			args := &AppendEntriesArgs{Term: term, LeaderID: r.id, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: leaderCommit}
			reply, err := r.transport.SendAppendEntries(ctx, peer, args)
			if err != nil {
				results <- false
				return
			}
			r.mu.Lock()
			r.stepDownIfBehind(reply.Term)
			stillLeader := r.role == Leader && r.currentTerm == term && reply.Term == term
			r.mu.Unlock()
			results <- stillLeader
		}(peer)
	}

	acks := 1 // self
	need := len(r.peers)/2 + 1
	if acks >= need {
		return true // single-node cluster
	}
	for i := 0; i < sent; i++ {
		select {
		case ok := <-results:
			if ok {
				acks++
				if acks >= need {
					return true
				}
			}
		case <-ctx.Done():
			return false
		}
	}
	return acks >= need
}

// base is the last snapshotted index; slice position of absolute index a is
// a - base. Caller holds mu.
func (r *Raft) base() uint64 { return r.log[0].Index }

func (r *Raft) lastLogIndex() uint64 { return r.log[len(r.log)-1].Index }

func (r *Raft) lastLogInfo() (index, term uint64) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

// entry returns the log entry at absolute index a (a must be > base and <= last).
// Caller holds mu.
func (r *Raft) entry(a uint64) LogEntry { return r.log[a-r.base()] }

// Submit appends a command if this node is the leader and kicks off replication.
func (r *Raft) Submit(command []byte) (index uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader {
		return 0, 0, false
	}
	index = r.lastLogIndex() + 1
	entry := LogEntry{Term: r.currentTerm, Index: index, Command: append([]byte(nil), command...)}
	r.log = append(r.log, entry)
	r.persistAppend([]LogEntry{entry})
	r.matchIndex[r.id] = index
	r.nextIndex[r.id] = index + 1
	r.maybeAdvanceCommit()
	r.broadcastAppendEntries()
	return index, r.currentTerm, true
}

// Snapshot compacts the log: the application has captured all state up to and
// including index in `data`, so Raft discards the log prefix through index.
func (r *Raft) Snapshot(index uint64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.base() || index > r.commitIndex {
		return // already compacted, or not yet committed
	}
	oldBase := r.base()
	term := r.log[index-oldBase].Term
	tail := append([]LogEntry(nil), r.log[index-oldBase+1:]...)
	r.log = append([]LogEntry{{Index: index, Term: term}}, tail...)
	r.snapshot = append([]byte(nil), data...)
	if err := r.persister.SaveSnapshot(Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: data}); err != nil {
		panic("raft: save snapshot: " + err.Error())
	}
	if err := r.persister.TruncatePrefix(index + 1); err != nil {
		panic("raft: truncate prefix: " + err.Error())
	}
}

// run is the single background loop.
func (r *Raft) run() {
	defer r.wg.Done()
	var lastHeartbeat time.Time
	for {
		select {
		case <-r.dead:
			return
		case <-time.After(tickInterval):
		}
		r.mu.Lock()
		if r.role == Leader {
			if time.Since(lastHeartbeat) >= heartbeatInterval {
				r.broadcastAppendEntries()
				lastHeartbeat = time.Now()
			}
		} else if !time.Now().Before(r.electionDeadline) {
			r.startElection()
		}
		r.mu.Unlock()
	}
}

// applier delivers committed entries (and installed snapshots) to applyCh in
// strict order. A pending snapshot is delivered before any later command.
func (r *Raft) applier() {
	defer r.wg.Done()
	r.mu.Lock()
	for {
		for r.pendingSnap == nil && r.lastApplied >= r.commitIndex && !r.killed() {
			r.applyCond.Wait()
		}
		if r.killed() {
			r.mu.Unlock()
			return
		}
		if r.pendingSnap != nil {
			snap := r.pendingSnap
			r.pendingSnap = nil
			if snap.index <= r.lastApplied {
				continue // superseded by commands we already applied
			}
			r.lastApplied = snap.index
			if r.commitIndex < snap.index {
				r.commitIndex = snap.index
			}
			msg := ApplyMsg{SnapshotValid: true, Snapshot: append([]byte(nil), snap.data...), SnapshotIndex: snap.index, SnapshotTerm: snap.term}
			r.mu.Unlock()
			select {
			case r.applyCh <- msg:
			case <-r.dead:
				return
			}
			r.mu.Lock()
			continue
		}
		r.lastApplied++
		e := r.entry(r.lastApplied)
		msg := ApplyMsg{CommandValid: true, Command: append([]byte(nil), e.Command...), Index: e.Index, Term: e.Term, NoOp: e.NoOp}
		r.mu.Unlock()
		select {
		case r.applyCh <- msg:
		case <-r.dead:
			return
		}
		r.mu.Lock()
	}
}

func (r *Raft) resetElectionTimer() {
	d := electionTimeoutMin + time.Duration(r.rng.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	r.electionDeadline = time.Now().Add(d)
}

func (r *Raft) persist() {
	if err := r.persister.SaveHardState(HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}); err != nil {
		panic("raft: persist failed: " + err.Error())
	}
}

// stepDownIfBehind implements Figure 2's "if RPC term T > currentTerm: set
// currentTerm = T, convert to follower". Caller holds mu.
func (r *Raft) stepDownIfBehind(term uint64) bool {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = NoVote
		r.role = Follower
		r.leaderID = -1
		r.resetElectionTimer()
		r.persist()
		return true
	}
	return false
}

// candidateUpToDate applies the §5.4.1 election restriction. Caller holds mu.
func (r *Raft) candidateUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	myIndex, myTerm := r.lastLogInfo()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}

// startElection converts to candidate and solicits votes. Caller holds mu.
func (r *Raft) startElection() {
	r.role = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.persist()
	r.resetElectionTimer()

	term := r.currentTerm
	lastIndex, lastTerm := r.lastLogInfo()
	votes := 1

	if votes*2 > len(r.peers) { // single-node cluster wins immediately
		r.becomeLeader()
		return
	}

	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{Term: term, CandidateID: r.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
			reply, err := r.transport.SendRequestVote(context.Background(), peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stepDownIfBehind(reply.Term) {
				return
			}
			if r.role != Candidate || r.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes*2 > len(r.peers) {
					r.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader initializes replication bookkeeping and asserts authority.
func (r *Raft) becomeLeader() {
	if r.role != Candidate {
		return
	}
	r.role = Leader
	r.leaderID = r.id
	n := len(r.peers)
	r.nextIndex = make([]uint64, n)
	r.matchIndex = make([]uint64, n)
	last := r.lastLogIndex()
	for _, p := range r.peers {
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	// Commit a no-op in this term (§8). It advances commitIndex to cover
	// recovered prior-term entries (making them re-apply immediately) and lets
	// ReadIndex serve linearizable reads once it commits.
	noop := LogEntry{Term: r.currentTerm, Index: last + 1, NoOp: true}
	r.log = append(r.log, noop)
	r.persistAppend([]LogEntry{noop})
	r.matchIndex[r.id] = last + 1
	r.nextIndex[r.id] = last + 2
	r.maybeAdvanceCommit()
	r.broadcastAppendEntries()
}

func (r *Raft) broadcastAppendEntries() {
	term := r.currentTerm
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go r.replicateTo(peer, term)
	}
}

// replicateTo sends one AppendEntries (or InstallSnapshot, if the follower needs
// a compacted prefix) and processes the reply.
func (r *Raft) replicateTo(peer int, term uint64) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}

	if r.nextIndex[peer] <= r.base() {
		// Follower needs entries we've compacted away — ship the snapshot.
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderID:          r.id,
			LastIncludedIndex: r.base(),
			LastIncludedTerm:  r.log[0].Term,
			Data:              append([]byte(nil), r.snapshot...),
		}
		r.mu.Unlock()
		reply, err := r.transport.SendInstallSnapshot(context.Background(), peer, args)
		if err != nil {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.stepDownIfBehind(reply.Term) {
			return
		}
		if r.role != Leader || r.currentTerm != term {
			return
		}
		if args.LastIncludedIndex > r.matchIndex[peer] {
			r.matchIndex[peer] = args.LastIncludedIndex
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.maybeAdvanceCommit()
		return
	}

	ni := r.nextIndex[peer]
	prevIndex := ni - 1
	prevTerm := r.entry(prevIndex).Term
	entries := append([]LogEntry(nil), r.log[ni-r.base():]...)
	leaderCommit := r.commitIndex
	r.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply, err := r.transport.SendAppendEntries(context.Background(), peer, args)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stepDownIfBehind(reply.Term) {
		return
	}
	if r.role != Leader || r.currentTerm != term {
		return
	}
	if reply.Success {
		match := prevIndex + uint64(len(entries))
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.maybeAdvanceCommit()
	} else if r.nextIndex[peer] == ni {
		r.nextIndex[peer] = r.backupIndex(reply, ni)
	}
}

// backupIndex computes the next nextIndex after a rejected AppendEntries using
// the follower's fast-conflict hints. Caller holds mu.
func (r *Raft) backupIndex(reply *AppendEntriesReply, ni uint64) uint64 {
	if reply.ConflictTerm == 0 {
		if reply.ConflictIndex >= 1 {
			return reply.ConflictIndex
		}
		return 1
	}
	for i := r.lastLogIndex(); i > r.base(); i-- {
		if r.entry(i).Term == reply.ConflictTerm {
			return i + 1
		}
		if r.entry(i).Term < reply.ConflictTerm {
			break
		}
	}
	if reply.ConflictIndex >= 1 {
		return reply.ConflictIndex
	}
	return 1
}

// maybeAdvanceCommit advances commitIndex to the highest current-term index
// replicated on a majority (§5.4.2). Caller holds mu; leader only.
func (r *Raft) maybeAdvanceCommit() {
	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		if r.entry(n).Term != r.currentTerm {
			break
		}
		count := 1
		for _, peer := range r.peers {
			if peer != r.id && r.matchIndex[peer] >= n {
				count++
			}
		}
		if count*2 > len(r.peers) {
			r.commitIndex = n
			r.applyCond.Broadcast()
			return
		}
	}
}
