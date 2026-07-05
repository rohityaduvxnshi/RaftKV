package api_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/api"
	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	"github.com/rohityaduvxnshi/RaftKV/internal/transport/inmem"
)

// apiCluster is an N-node cluster where each node runs a Raft node behind an
// api.Server, on the simulated in-mem network.
type apiCluster struct {
	t         *testing.T
	n         int
	net       *inmem.Network
	rafts     []*raft.Raft
	servers   []*api.Server
	connected []bool
}

func makeAPICluster(t *testing.T, n int, seed int64) *apiCluster {
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	c := &apiCluster{
		t:         t,
		n:         n,
		net:       inmem.NewNetwork(seed),
		rafts:     make([]*raft.Raft, n),
		servers:   make([]*api.Server, n),
		connected: make([]bool, n),
	}
	for i := 0; i < n; i++ {
		applyCh := make(chan raft.ApplyMsg, 256)
		store := kv.New()
		rf := raft.New(raft.Config{
			ID:        i,
			Peers:     peers,
			Transport: c.net.Transport(i),
			Persister: raft.NewMemPersister(),
			ApplyCh:   applyCh,
			Seed:      seed,
		})
		c.net.Register(i, rf)
		c.rafts[i] = rf
		c.servers[i] = api.NewServer(i, rf, store, applyCh)
		c.connected[i] = true
	}
	for i := 0; i < n; i++ {
		c.rafts[i].Start()
	}
	return c
}

func (c *apiCluster) cleanup() {
	for _, rf := range c.rafts {
		rf.Kill() // stop appliers before closing the servers that drain them
	}
	for _, s := range c.servers {
		s.Close()
	}
}

func (c *apiCluster) disconnect(i int) { c.connected[i] = false; c.net.SetConnected(i, false) }
func (c *apiCluster) connect(i int)    { c.connected[i] = true; c.net.SetConnected(i, true) }

// leader waits for and returns a connected leader.
func (c *apiCluster) leader() int {
	c.t.Helper()
	for iter := 0; iter < 50; iter++ {
		time.Sleep(60 * time.Millisecond)
		for i := 0; i < c.n; i++ {
			if !c.connected[i] {
				continue
			}
			if _, isLeader := c.rafts[i].GetState(); isLeader {
				return i
			}
		}
	}
	c.t.Fatal("no leader elected")
	return -1
}

func bg() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

// TestExactlyOnceRetry: re-submitting the same (clientID, seqNo) command applies
// it only once. Append is non-idempotent, so a double-apply would be visible.
func TestExactlyOnceRetry(t *testing.T) {
	c := makeAPICluster(t, 3, 501)
	defer c.cleanup()
	ldr := c.leader()
	ctx, cancel := bg()
	defer cancel()

	v1, err := c.servers[ldr].Append(ctx, "client-1", 1, "log", "A")
	if err != nil || v1 != "A" {
		t.Fatalf("first append: v=%q err=%v, want A", v1, err)
	}
	// Retry the identical request (as if the ack was lost).
	v2, err := c.servers[ldr].Append(ctx, "client-1", 1, "log", "A")
	if err != nil || v2 != "A" {
		t.Fatalf("retried append: v=%q err=%v, want A (cached, not applied twice)", v2, err)
	}
	// A genuinely new request advances.
	v3, err := c.servers[ldr].Append(ctx, "client-1", 2, "log", "B")
	if err != nil || v3 != "AB" {
		t.Fatalf("next append: v=%q err=%v, want AB", v3, err)
	}
	val, _, _ := c.servers[ldr].Get(ctx, "log")
	if val != "AB" {
		t.Fatalf("log=%q, want AB — a retry was applied more than once", val)
	}
}

// TestSingleNodeAPI: on a 1-node cluster the commit is synchronous inside
// Submit, so the apply can race a write's waiter registration. A committed write
// must still be delivered, not spuriously time out.
func TestSingleNodeAPI(t *testing.T) {
	c := makeAPICluster(t, 1, 505)
	defer c.cleanup()
	c.leader()
	ctx, cancel := bg()
	defer cancel()
	if err := c.servers[0].Put(ctx, "c1", 1, "k", "v"); err != nil {
		t.Fatalf("single-node put: %v", err)
	}
	v, found, err := c.servers[0].Get(ctx, "k")
	if err != nil || !found || v != "v" {
		t.Fatalf("single-node get: v=%q found=%v err=%v, want v", v, found, err)
	}
}

// TestZeroSeqDedup: a client whose sequence numbers start at 0 still gets
// exactly-once semantics on retry.
func TestZeroSeqDedup(t *testing.T) {
	c := makeAPICluster(t, 3, 506)
	defer c.cleanup()
	ldr := c.leader()
	ctx, cancel := bg()
	defer cancel()
	if _, err := c.servers[ldr].Append(ctx, "c1", 0, "k", "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.servers[ldr].Append(ctx, "c1", 0, "k", "A"); err != nil { // retry
		t.Fatal(err)
	}
	if v, _, _ := c.servers[ldr].Get(ctx, "k"); v != "A" {
		t.Fatalf("k=%q, want A (seq-0 retry must be deduped)", v)
	}
}

// TestNoStaleRead: a leader isolated into a minority must not serve a read from
// its stale local state. Its ReadIndex leadership confirmation fails (no quorum),
// so the read is rejected while the majority's new leader serves the fresh value.
func TestNoStaleRead(t *testing.T) {
	c := makeAPICluster(t, 5, 502)
	defer c.cleanup()
	old := c.leader()
	ctx, cancel := bg()
	defer cancel()
	if err := c.servers[old].Put(ctx, "c1", 1, "k", "v1"); err != nil {
		t.Fatalf("initial put: %v", err)
	}

	// Isolate the old leader; the majority elects a new one and moves on.
	c.disconnect(old)
	fresh := c.leader() // among the connected majority
	if fresh == old {
		t.Fatal("isolated node still reported as leader")
	}
	if err := c.servers[fresh].Put(ctx, "c2", 1, "k", "v2"); err != nil {
		t.Fatalf("new-leader put: %v", err)
	}

	// The old, partitioned leader must NOT serve the stale "v1".
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if v, _, err := c.servers[old].Get(rctx, "k"); err == nil {
		t.Fatalf("stale read: isolated leader returned %q instead of refusing", v)
	}

	// The current leader serves the fresh value.
	v, found, err := c.servers[fresh].Get(ctx, "k")
	if err != nil || !found || v != "v2" {
		t.Fatalf("fresh read: v=%q found=%v err=%v, want v2", v, found, err)
	}
}

// TestWriteToFollowerRedirects: a write to a non-leader is refused with a hint to
// the current leader.
func TestWriteToFollowerRedirects(t *testing.T) {
	c := makeAPICluster(t, 3, 503)
	defer c.cleanup()
	ldr := c.leader()
	follower := (ldr + 1) % c.n
	ctx, cancel := bg()
	defer cancel()

	err := c.servers[follower].Put(ctx, "c1", 1, "k", "v")
	if err != api.ErrNotLeader {
		t.Fatalf("write to follower: got %v, want ErrNotLeader", err)
	}
	if got := c.servers[follower].Leader(); got != ldr {
		t.Fatalf("follower's leader hint = %d, want %d", got, ldr)
	}
}

// TestHTTPRoundTrip exercises the HTTP handler end-to-end against the leader.
func TestHTTPRoundTrip(t *testing.T) {
	c := makeAPICluster(t, 3, 504)
	defer c.cleanup()
	ldr := c.leader()
	ts := httptest.NewServer(api.NewHTTPHandler(c.servers[ldr], nil))
	defer ts.Close()

	// PUT /kv/foo with a client session.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	req.Header.Set("X-Client-Id", "c1")
	req.Header.Set("X-Seq-No", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT: status=%v err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// GET /kv/foo (linearizable read).
	resp, err = http.Get(ts.URL + "/kv/foo")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: status=%v err=%v", resp.StatusCode, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `"bar"`) {
		t.Fatalf("GET body = %s, want value bar", body)
	}
}
