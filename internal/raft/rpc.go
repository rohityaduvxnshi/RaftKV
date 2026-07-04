package raft

// RPC handlers (inbound). *Raft satisfies raft.RPCHandler. Every handler first
// applies Figure 2's "step down if the caller's term is higher" rule, then
// rejects anything from a stale term.

var _ RPCHandler = (*Raft)(nil)

// HandleRequestVote grants a vote at most once per term, and only to a candidate
// whose log is at least as up-to-date as ours (§5.4.1).
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)

	reply := &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	if args.Term < r.currentTerm {
		return reply
	}
	if (r.votedFor == NoVote || r.votedFor == args.CandidateID) &&
		r.candidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		r.votedFor = args.CandidateID
		r.persist()
		r.resetElectionTimer() // don't start our own election right after voting
		reply.VoteGranted = true
	}
	return reply
}

// HandleAppendEntries is the Phase 1 heartbeat path: accept from a leader whose
// term is current, step down if we were a candidate, and refresh the election
// timer. Log consistency checking arrives in Phase 2.
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)

	reply := &AppendEntriesReply{Term: r.currentTerm, Success: false}
	if args.Term < r.currentTerm {
		return reply // reject a stale leader
	}
	// Valid leader for the current term.
	r.role = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimer()
	reply.Success = true
	return reply
}

// HandleInstallSnapshot is a Phase 1 stub; it still honors the term rule so a
// higher-term snapshot RPC makes us step down. Real handling lands in Phase 4.
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)
	return &InstallSnapshotReply{Term: r.currentTerm}
}
