package grpc_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	grpctransport "github.com/rohityaduvxnshi/RaftKV/internal/transport/grpc"
)

// TestGRPCReplication stands up a 3-node cluster on real gRPC over localhost and
// verifies the same core elects a leader and replicates committed writes to every
// node — i.e. the gRPC transport is interchangeable with the in-mem one.
func TestGRPCReplication(t *testing.T) {
	const n = 3
	peers := []int{0, 1, 2}

	// Bind listeners first so every node's address is known before we wire the
	// transports (which need the full addr map).
	lis := make([]net.Listener, n)
	addrs := make(map[int]string, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lis[i] = l
		addrs[i] = l.Addr().String()
	}

	rafts := make([]*raft.Raft, n)
	servers := make([]*grpctransport.Server, n)
	var mu sync.Mutex
	applied := make([]int, n) // per-node count of applied (non-no-op) commands
	done := make(chan struct{})

	for i := 0; i < n; i++ {
		i := i
		applyCh := make(chan raft.ApplyMsg, 256)
		rf := raft.New(raft.Config{
			ID:        i,
			Peers:     peers,
			Transport: grpctransport.NewTransport(i, addrs),
			Persister: raft.NewMemPersister(),
			ApplyCh:   applyCh,
			Seed:      1,
		})
		rafts[i] = rf
		servers[i] = grpctransport.NewServer(rf)
		go func() { _ = servers[i].Serve(lis[i]) }()
		go func() {
			for {
				select {
				case msg := <-applyCh:
					if msg.CommandValid && !msg.NoOp {
						mu.Lock()
						applied[i]++
						mu.Unlock()
					}
				case <-done:
					return
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		rafts[i].Start()
	}
	defer func() {
		for i := 0; i < n; i++ {
			rafts[i].Kill()
			servers[i].Stop()
		}
		close(done)
	}()

	// A leader must emerge over real gRPC.
	leader := -1
	for iter := 0; iter < 60 && leader < 0; iter++ {
		time.Sleep(60 * time.Millisecond)
		for i := 0; i < n; i++ {
			if _, isLeader := rafts[i].GetState(); isLeader {
				leader = i
				break
			}
		}
	}
	if leader < 0 {
		t.Fatal("no leader elected over gRPC")
	}

	// Submit writes through the leader (re-finding it if leadership moves).
	const writes = 5
	for k := 0; k < writes; k++ {
		cmd := kv.EncodePut("k", fmt.Sprintf("v%d", k))
		submitted := false
		for try := 0; try < 50 && !submitted; try++ {
			if _, _, ok := rafts[leader].Submit(cmd); ok {
				submitted = true
				break
			}
			for i := 0; i < n; i++ {
				if _, isLeader := rafts[i].GetState(); isLeader {
					leader = i
				}
			}
			time.Sleep(40 * time.Millisecond)
		}
		if !submitted {
			t.Fatalf("could not submit write %d", k)
		}
	}

	// Every node must apply all writes (replicated + committed over gRPC).
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		lo := applied[0]
		for i := 1; i < n; i++ {
			if applied[i] < lo {
				lo = applied[i]
			}
		}
		mu.Unlock()
		if lo >= writes {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d writes replicated to all nodes over gRPC", lo, writes)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
