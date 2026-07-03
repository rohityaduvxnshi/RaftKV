package raft

import "sync"

// MemPersister is an in-memory Persister for tests. It is safe for concurrent
// use. It provides no actual durability (nothing survives process exit) but
// exercises the exact same interface and semantics as the bbolt-backed
// Persister, including truncation, so the Raft core can be tested without disk.
type MemPersister struct {
	mu       sync.Mutex
	hs       HardState
	hasState bool
	entries  []LogEntry // ordered by Index, ascending, contiguous
	snap     Snapshot
	hasSnap  bool
	bytes    uint64
}

// NewMemPersister returns an empty in-memory Persister.
func NewMemPersister() *MemPersister {
	return &MemPersister{hs: HardState{VotedFor: NoVote}}
}

var _ Persister = (*MemPersister)(nil)

func (p *MemPersister) SaveHardState(hs HardState) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hs = hs
	p.hasState = true
	return nil
}

func (p *MemPersister) AppendEntries(entries []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range entries {
		p.entries = append(p.entries, e)
		p.bytes += entryBytes(e)
	}
	return nil
}

func (p *MemPersister) TruncateSuffix(index uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keep := p.entries[:0:0]
	for _, e := range p.entries {
		if e.Index < index {
			keep = append(keep, e)
		} else {
			p.bytes -= entryBytes(e)
		}
	}
	p.entries = keep
	return nil
}

func (p *MemPersister) TruncatePrefix(index uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keep := p.entries[:0:0]
	for _, e := range p.entries {
		if e.Index >= index {
			keep = append(keep, e)
		} else {
			p.bytes -= entryBytes(e)
		}
	}
	p.entries = keep
	return nil
}

func (p *MemPersister) SaveSnapshot(snap Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snap = snap
	p.hasSnap = true
	return nil
}

func (p *MemPersister) Load() (PersistentState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries := make([]LogEntry, len(p.entries))
	copy(entries, p.entries)
	return PersistentState{
		HardState: p.hs,
		HasState:  p.hasState,
		Entries:   entries,
		Snapshot:  p.snap,
		HasSnap:   p.hasSnap,
	}, nil
}

func (p *MemPersister) LogBytes() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bytes
}

func (p *MemPersister) Close() error { return nil }

// entryBytes approximates an entry's on-disk footprint: the command plus fixed
// overhead for the term/index headers. Good enough for compaction thresholds.
func entryBytes(e LogEntry) uint64 { return uint64(len(e.Command)) + 16 }
