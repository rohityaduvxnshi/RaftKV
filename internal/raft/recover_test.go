package raft

import "testing"

// TestRecoverTornSnapshot simulates a crash between SaveSnapshot and the log
// prefix truncation (two separate persister transactions): the disk holds a
// snapshot at index 5 AND the full, un-truncated log 1..10. On restart, New must
// reconcile to a contiguous log [boundary@5, 6..10] — never [boundary@5, 1..10],
// which would break the log[i].Index == base()+i invariant.
func TestRecoverTornSnapshot(t *testing.T) {
	p := NewMemPersister()
	entries := make([]LogEntry, 0, 10)
	for i := 1; i <= 10; i++ {
		entries = append(entries, LogEntry{Term: 1, Index: uint64(i), Command: []byte{byte(i)}})
	}
	if err := p.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	if err := p.SaveSnapshot(Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 1, Data: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	_ = p.SaveHardState(HardState{CurrentTerm: 2, VotedFor: NoVote})
	// Deliberately NO TruncatePrefix — that is the crash window.

	r := New(Config{ID: 0, Peers: []int{0}, Persister: p})

	if r.base() != 5 {
		t.Fatalf("base = %d, want 5", r.base())
	}
	if got := r.lastLogIndex(); got != 10 {
		t.Fatalf("lastLogIndex = %d, want 10", got)
	}
	if len(r.log) != 6 {
		t.Fatalf("log len = %d, want 6 (boundary + entries 6..10)", len(r.log))
	}
	for i := range r.log {
		if want := r.base() + uint64(i); r.log[i].Index != want {
			t.Fatalf("log[%d].Index = %d, want %d — non-contiguous after torn-snapshot recovery", i, r.log[i].Index, want)
		}
	}
}
