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

// ApplyMsg is a committed entry delivered, in index order, to the state machine
// via the apply channel.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Config wires up a Raft node. Peers lists every node ID in the cluster,
// including this node's own ID.
type Config struct {
	ID        int
	Peers     []int
	Transport Transport
	Persister Persister
	ApplyCh   chan ApplyMsg // committed entries are delivered here in order
	Seed      int64         // per-node RNG seed for election-timeout jitter
}

// Raft is a single node of the cluster. All mutable state is guarded by mu.
// The one rule that keeps it deadlock-free: outbound RPCs are always sent from
// goroutines that do NOT hold mu; those goroutines re-acquire mu only to process
// the reply.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int
	transport Transport
	persister Persister

	// Persistent state (Figure 2). Durably saved before responding to RPCs.
	// The log carries a sentinel entry at index 0 ({Term:0,Index:0}) so that
	// log[i].Index == i and prevLogIndex lookups need no special-casing.
	currentTerm uint64
	votedFor    int
	log         []LogEntry

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

	applyCh   chan ApplyMsg
	applyCond *sync.Cond // signalled when commitIndex advances (or on Kill)

	dead chan struct{}
	wg   sync.WaitGroup
}

// New constructs a node in the Follower role. It does not start any goroutines;
// register it with the transport first, then call Start.
func New(cfg Config) *Raft {
	r := &Raft{
		id:          cfg.ID,
		peers:       append([]int(nil), cfg.Peers...),
		transport:   cfg.Transport,
		persister:   cfg.Persister,
		applyCh:     cfg.ApplyCh,
		currentTerm: 0,
		votedFor:    NoVote,
		log:         []LogEntry{{Term: 0, Index: 0}}, // sentinel
		role:        Follower,
		leaderID:    -1,
		rng:         rand.New(rand.NewSource(cfg.Seed + int64(cfg.ID)*2654435761)),
		dead:        make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	// Phase 3 loads persisted state here; for now start empty.
	r.resetElectionTimer()
	return r
}

// Start launches the background loops: the election/heartbeat loop always, and
// the apply loop when an apply channel is configured.
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
	r.applyCond.Broadcast() // wake the applier so it observes the shutdown
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

// Submit appends a command to the log if this node is the leader and kicks off
// replication. It returns the index the command will occupy once committed, the
// current term, and whether this node is the leader. It does NOT wait for commit.
func (r *Raft) Submit(command []byte) (index uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader {
		return 0, 0, false
	}
	index = r.lastLogIndex() + 1
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Index: index, Command: append([]byte(nil), command...)})
	r.matchIndex[r.id] = index
	r.nextIndex[r.id] = index + 1
	// Phase 3 persists the new entry here before it may count toward commit.
	// Advance commit now so a single-node cluster (no followers to reply) still
	// makes progress; a no-op for larger clusters until replicas ack.
	r.maybeAdvanceCommit()
	r.broadcastAppendEntries()
	return index, r.currentTerm, true
}

// run is the single background loop. As leader it replicates (which doubles as
// heartbeating) every heartbeatInterval; otherwise it starts an election once
// its randomized election deadline passes.
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

// applier delivers newly-committed entries to applyCh in strict index order.
func (r *Raft) applier() {
	defer r.wg.Done()
	r.mu.Lock()
	for {
		for r.lastApplied >= r.commitIndex && !r.killed() {
			r.applyCond.Wait()
		}
		if r.killed() {
			r.mu.Unlock()
			return
		}
		r.lastApplied++
		e := r.log[r.lastApplied]
		msg := ApplyMsg{Index: e.Index, Term: e.Term, Command: append([]byte(nil), e.Command...)}
		r.mu.Unlock()
		select {
		case r.applyCh <- msg:
		case <-r.dead:
			return // mu already released
		}
		r.mu.Lock()
	}
}

// resetElectionTimer schedules the next timeout at a fresh random point in
// [min,max). Caller holds mu.
func (r *Raft) resetElectionTimer() {
	d := electionTimeoutMin + time.Duration(r.rng.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	r.electionDeadline = time.Now().Add(d)
}

// lastLogIndex / lastLogInfo read the tail of the log (the sentinel guarantees
// there is always at least one entry). Caller holds mu.
func (r *Raft) lastLogIndex() uint64 { return r.log[len(r.log)-1].Index }

func (r *Raft) lastLogInfo() (index, term uint64) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

// persist durably saves the Figure-2 persistent state. A durability failure is
// unrecoverable — a node that cannot persist cannot safely participate — so we
// stop hard rather than risk a safety violation. Caller holds mu.
func (r *Raft) persist() {
	if err := r.persister.SaveHardState(HardState{CurrentTerm: r.currentTerm, VotedFor: r.votedFor}); err != nil {
		panic("raft: persist failed: " + err.Error())
	}
}

// stepDownIfBehind implements Figure 2's "if RPC term T > currentTerm: set
// currentTerm = T, convert to follower". Returns true if it stepped down.
// Caller holds mu.
func (r *Raft) stepDownIfBehind(term uint64) bool {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = NoVote
		r.role = Follower
		r.leaderID = -1
		// A node that just learned of a higher term should wait a fresh
		// randomized timeout before campaigning. Inbound RPC handlers reset the
		// timer themselves, but the RPC-reply step-down paths (vote/heartbeat
		// replies) do not — without this a just-deposed leader, whose deadline
		// is always stale, would re-campaign on the very next tick.
		r.resetElectionTimer()
		r.persist()
		return true
	}
	return false
}

// candidateUpToDate applies the §5.4.1 election restriction: a candidate's log
// must be at least as up-to-date as ours to earn a vote. Caller holds mu.
func (r *Raft) candidateUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	myIndex, myTerm := r.lastLogInfo()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}

// startElection converts to candidate for a new term and solicits votes. Caller
// holds mu; vote replies are processed on separate goroutines.
func (r *Raft) startElection() {
	r.role = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.persist()
	r.resetElectionTimer()

	term := r.currentTerm
	lastIndex, lastTerm := r.lastLogInfo()
	votes := 1 // vote for self

	// In a single-node cluster the self-vote is already a majority; there are no
	// peers to reply and drive becomeLeader, so win immediately.
	if votes*2 > len(r.peers) {
		r.becomeLeader()
		return
	}

	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
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
				return // stale reply
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

// becomeLeader transitions to leader, initializes per-follower replication
// bookkeeping, and immediately establishes authority with a replication round.
// Caller holds mu.
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
	r.matchIndex[r.id] = last
	r.broadcastAppendEntries()
}

// broadcastAppendEntries starts a replication round to every follower. Caller
// holds mu; each peer is handled on its own goroutine.
func (r *Raft) broadcastAppendEntries() {
	term := r.currentTerm
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go r.replicateTo(peer, term)
	}
}

// replicateTo sends one AppendEntries to peer with everything from nextIndex
// onward (empty ⇒ a heartbeat), then processes the reply: advance match/next on
// success, or back up nextIndex via the fast-conflict hints on failure.
func (r *Raft) replicateTo(peer int, term uint64) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevIndex := ni - 1
	prevTerm := r.log[prevIndex].Term
	// Everything from nextIndex onward (a copy, so the follower's gob-encode
	// can't race the leader mutating its log). Empty ⇒ a heartbeat.
	entries := append([]LogEntry(nil), r.log[ni:]...)
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
		return // stale reply
	}
	if reply.Success {
		match := prevIndex + uint64(len(entries))
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.maybeAdvanceCommit()
	} else if r.nextIndex[peer] == ni {
		// Only back up if nextIndex hasn't moved since we sent (avoids thrashing
		// on reordered replies).
		r.nextIndex[peer] = r.backupIndex(reply, ni)
	}
}

// backupIndex computes the next nextIndex after a rejected AppendEntries using
// the follower's fast-conflict hints (§5.3 optimization). Caller holds mu.
func (r *Raft) backupIndex(reply *AppendEntriesReply, ni uint64) uint64 {
	if reply.ConflictTerm == 0 {
		// Follower's log is shorter than prevLogIndex; jump to its end.
		if reply.ConflictIndex >= 1 {
			return reply.ConflictIndex
		}
		return 1
	}
	// Find the last entry in our log with term == ConflictTerm.
	for i := r.lastLogIndex(); i >= 1; i-- {
		if r.log[i].Term == reply.ConflictTerm {
			return i + 1
		}
		if r.log[i].Term < reply.ConflictTerm {
			break
		}
	}
	// We don't have that term: fall back to the follower's first index for it.
	if reply.ConflictIndex >= 1 {
		return reply.ConflictIndex
	}
	return 1
}

// maybeAdvanceCommit advances commitIndex to the highest index replicated on a
// majority whose entry is from the current term (§5.4.2). Caller holds mu; leader
// only.
func (r *Raft) maybeAdvanceCommit() {
	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			break // terms are non-decreasing; older-term entries can't commit directly
		}
		count := 1 // self
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
