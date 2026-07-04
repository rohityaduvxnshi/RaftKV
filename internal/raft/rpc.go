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
		r.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

// HandleAppendEntries runs the log-matching check, splices in entries, and
// advances the follower's commit index. Indices are offset by base() (the
// snapshot boundary); a prevLogIndex inside our snapshot is fast-forwarded.
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)

	reply := &AppendEntriesReply{Term: r.currentTerm, Success: false}
	if args.Term < r.currentTerm {
		return reply
	}
	r.role = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimer()

	// If prevLogIndex lands inside our snapshot, drop the entries it already
	// covers and pretend the RPC started at our snapshot boundary.
	if args.PrevLogIndex < r.base() {
		covered := r.base() - args.PrevLogIndex
		if covered >= uint64(len(args.Entries)) {
			args.Entries = nil
		} else {
			args.Entries = args.Entries[covered:]
		}
		args.PrevLogIndex = r.base()
		args.PrevLogTerm = r.log[0].Term
	}

	last := r.lastLogIndex()
	if args.PrevLogIndex > last {
		reply.ConflictTerm = 0
		reply.ConflictIndex = last + 1
		return reply
	}
	if r.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = r.entry(args.PrevLogIndex).Term
		i := args.PrevLogIndex
		for i > r.base()+1 && r.entry(i-1).Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Splice in new entries, truncating only at the first genuine conflict.
	for j := 0; j < len(args.Entries); j++ {
		idx := args.PrevLogIndex + 1 + uint64(j)
		if idx <= last {
			if r.entry(idx).Term == args.Entries[j].Term {
				continue
			}
			r.log = r.log[:idx-r.base()]
			r.persistTruncateSuffix(idx)
		}
		r.log = append(r.log, args.Entries[j:]...)
		r.persistAppend(args.Entries[j:])
		break
	}

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

// HandleInstallSnapshot installs a leader's snapshot on a follower that has
// fallen too far behind, discarding the covered log prefix and handing the
// snapshot to the state machine via the apply channel.
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stepDownIfBehind(args.Term)

	reply := &InstallSnapshotReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply
	}
	r.role = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimer()

	if args.LastIncludedIndex <= r.base() {
		return reply // stale — we already have this snapshot or better
	}

	// Keep any already-present, matching suffix beyond the snapshot; otherwise
	// discard the whole log.
	if args.LastIncludedIndex < r.lastLogIndex() && r.entry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		tail := append([]LogEntry(nil), r.log[args.LastIncludedIndex-r.base()+1:]...)
		r.log = append([]LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}, tail...)
		r.snapshot = append([]byte(nil), args.Data...)
		r.saveSnapshotLocked(args)
		if err := r.persister.TruncatePrefix(args.LastIncludedIndex + 1); err != nil {
			panic("raft: truncate prefix: " + err.Error())
		}
	} else {
		r.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
		r.snapshot = append([]byte(nil), args.Data...)
		r.saveSnapshotLocked(args)
		// Clear the whole persisted log; only the snapshot remains.
		if err := r.persister.TruncateSuffix(1); err != nil {
			panic("raft: truncate suffix: " + err.Error())
		}
	}

	if r.commitIndex < args.LastIncludedIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	r.pendingSnap = &pendingSnap{args.LastIncludedIndex, args.LastIncludedTerm, args.Data}
	r.applyCond.Broadcast()
	return reply
}

func (r *Raft) saveSnapshotLocked(args *InstallSnapshotArgs) {
	if err := r.persister.SaveSnapshot(Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}); err != nil {
		panic("raft: save snapshot: " + err.Error())
	}
}
