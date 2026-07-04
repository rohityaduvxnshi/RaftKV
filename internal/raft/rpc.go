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

// HandleAppendEntries runs the log-matching consistency check, appends/overwrites
// entries, and advances the follower's commit index. On rejection it returns the
// fast-conflict hints (ConflictTerm/ConflictIndex) so the leader can back up by a
// whole term in one round trip.
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)

	reply := &AppendEntriesReply{Term: r.currentTerm, Success: false}
	if args.Term < r.currentTerm {
		return reply // reject a stale leader
	}
	// Valid leader for the current term: (re)assert follower status and defer the
	// election timeout.
	r.role = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimer()

	last := r.lastLogIndex()

	// Consistency check at PrevLogIndex.
	if args.PrevLogIndex > last {
		// Our log is too short; tell the leader where it ends.
		reply.ConflictTerm = 0
		reply.ConflictIndex = last + 1
		return reply
	}
	if r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Term mismatch: report the conflicting term and its first index so the
		// leader can skip the whole term.
		reply.ConflictTerm = r.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 1 && r.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Log matches through PrevLogIndex. Splice in the new entries, truncating
	// only at the first genuine conflict (so reordered/duplicate AppendEntries
	// don't drop entries we've already matched).
	for j := 0; j < len(args.Entries); j++ {
		idx := args.PrevLogIndex + 1 + uint64(j)
		if idx <= last {
			if r.log[idx].Term == args.Entries[j].Term {
				continue // already have this entry
			}
			r.log = r.log[:idx]          // conflict: drop this and everything after
			r.persistTruncateSuffix(idx) // durable before we ack
		}
		r.log = append(r.log, args.Entries[j:]...)
		r.persistAppend(args.Entries[j:]) // durable before we ack
		break
	}

	// Advance commit toward the leader's, bounded by the index of the last entry
	// in THIS AppendEntries (Figure 2) — not our own last index, which may hold
	// later, still-uncommitted entries from a previous leader.
	if args.LeaderCommit > r.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		newCommit := args.LeaderCommit
		if newCommit > lastNew {
			newCommit = lastNew
		}
		if newCommit > r.commitIndex {
			r.commitIndex = newCommit
			r.applyCond.Broadcast()
		}
	}

	reply.Success = true
	return reply
}

// HandleInstallSnapshot is a Phase 1/2 stub; it still honors the term rule so a
// higher-term snapshot RPC makes us step down. Real handling lands in Phase 4.
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)
	return &InstallSnapshotReply{Term: r.currentTerm}
}
