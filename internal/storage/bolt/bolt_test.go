package bolt

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
)

// TestPersisterRoundTrip: hard state, log, and snapshot survive close + reopen
// (real on-disk durability).
func TestPersisterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	p, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{CurrentTerm: 7, VotedFor: 2}
	if err := p.SaveHardState(hs); err != nil {
		t.Fatal(err)
	}
	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 2, Index: 3, Command: []byte("c")},
	}
	if err := p.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	snap := raft.Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("snap")}
	if err := p.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: state must come back from disk.
	p2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	ps, err := p2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !ps.HasState || ps.HardState != hs {
		t.Fatalf("hard state = %+v (has=%v), want %+v", ps.HardState, ps.HasState, hs)
	}
	if !reflect.DeepEqual(ps.Entries, entries) {
		t.Fatalf("entries = %+v, want %+v", ps.Entries, entries)
	}
	if !ps.HasSnap || ps.Snapshot.LastIncludedIndex != 2 || !bytes.Equal(ps.Snapshot.Data, snap.Data) {
		t.Fatalf("snapshot = %+v (has=%v), want %+v", ps.Snapshot, ps.HasSnap, snap)
	}
}

func mkEntries(n int) []raft.LogEntry {
	es := make([]raft.LogEntry, n)
	for i := 0; i < n; i++ {
		es[i] = raft.LogEntry{Term: 1, Index: uint64(i + 1)}
	}
	return es
}

// TestTruncateSuffix drops entries with Index >= the given index.
func TestTruncateSuffix(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.AppendEntries(mkEntries(4)); err != nil {
		t.Fatal(err)
	}
	if err := p.TruncateSuffix(3); err != nil { // drop 3,4
		t.Fatal(err)
	}
	ps, _ := p.Load()
	if len(ps.Entries) != 2 || ps.Entries[1].Index != 2 {
		t.Fatalf("after TruncateSuffix(3): %+v, want indices [1,2]", ps.Entries)
	}
}

// TestTruncatePrefix drops entries with Index < the given index.
func TestTruncatePrefix(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.AppendEntries(mkEntries(4)); err != nil {
		t.Fatal(err)
	}
	if err := p.TruncatePrefix(3); err != nil { // drop 1,2
		t.Fatal(err)
	}
	ps, _ := p.Load()
	if len(ps.Entries) != 2 || ps.Entries[0].Index != 3 {
		t.Fatalf("after TruncatePrefix(3): %+v, want indices [3,4]", ps.Entries)
	}
}
