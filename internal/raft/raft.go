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

// Config wires up a Raft node. Peers lists every node ID in the cluster,
// including this node's own ID.
type Config struct {
	ID        int
	Peers     []int
	Transport Transport
	Persister Persister
	Seed      int64 // per-node RNG seed for election-timeout jitter (reproducible)
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
	currentTerm uint64
	votedFor    int
	log         []LogEntry // real entries arrive in Phase 2; empty here

	// Volatile state.
	role     Role
	leaderID int

	electionDeadline time.Time
	rng              *rand.Rand

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
		currentTerm: 0,
		votedFor:    NoVote,
		role:        Follower,
		leaderID:    -1,
		rng:         rand.New(rand.NewSource(cfg.Seed + int64(cfg.ID)*2654435761)),
		dead:        make(chan struct{}),
	}
	// Phase 3 loads persisted state here; for now start empty.
	r.resetElectionTimer()
	return r
}

// Start launches the background election/heartbeat loop.
func (r *Raft) Start() {
	r.wg.Add(1)
	go r.run()
}

// Kill stops the node and waits for its background loop to exit. Call once.
func (r *Raft) Kill() {
	close(r.dead)
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

// run is the single background loop. As leader it sends heartbeats every
// heartbeatInterval; otherwise it starts an election once its (randomized)
// election deadline passes. One loop avoids dynamic goroutine bookkeeping and
// the WaitGroup.Add/Wait races that come with it.
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
				r.broadcastHeartbeat(r.currentTerm)
				lastHeartbeat = time.Now()
			}
		} else if !time.Now().Before(r.electionDeadline) {
			r.startElection()
		}
		r.mu.Unlock()
	}
}

// resetElectionTimer schedules the next timeout at a fresh random point in
// [min,max). Caller holds mu.
func (r *Raft) resetElectionTimer() {
	d := electionTimeoutMin + time.Duration(r.rng.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	r.electionDeadline = time.Now().Add(d)
}

// lastLogInfo returns the index and term of the last log entry (0,0 when empty).
// Caller holds mu.
func (r *Raft) lastLogInfo() (index, term uint64) {
	if len(r.log) == 0 {
		return 0, 0
	}
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
			// Ignore stale replies: we may have moved on to another term/role.
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

// becomeLeader transitions to leader. The background loop picks this up and
// starts heartbeating on its next tick. Caller holds mu.
func (r *Raft) becomeLeader() {
	if r.role != Candidate {
		return
	}
	r.role = Leader
	r.leaderID = r.id
	// Phase 2 initializes nextIndex/matchIndex here.
}

// broadcastHeartbeat sends one round of empty AppendEntries to all peers. Caller
// holds mu; sends happen on separate goroutines.
func (r *Raft) broadcastHeartbeat(term uint64) {
	prevIndex, prevTerm := r.lastLogInfo()
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go func(peer int) {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     r.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
			}
			reply, err := r.transport.SendAppendEntries(context.Background(), peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			r.stepDownIfBehind(reply.Term)
		}(peer)
	}
}
