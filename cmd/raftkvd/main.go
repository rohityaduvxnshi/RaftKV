// Command raftkvd runs a single RaftKV server node: the Raft core over the gRPC
// transport (inter-node), a bbolt persister (durable log), the KV state machine,
// and the HTTP client API. Run N of them with a shared -peers list to form a
// cluster.
//
//	raftkvd -id 0 -peers 0@raft0:9090,1@raft1:9090,2@raft2:9090 \
//	        -http-addr :8080 -data-dir /data
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rohityaduvxnshi/RaftKV/internal/api"
	"github.com/rohityaduvxnshi/RaftKV/internal/kv"
	"github.com/rohityaduvxnshi/RaftKV/internal/observability"
	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
	boltstore "github.com/rohityaduvxnshi/RaftKV/internal/storage/bolt"
	grpctransport "github.com/rohityaduvxnshi/RaftKV/internal/transport/grpc"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	id := flag.Int("id", 0, "this node's ID (dense, 0-based)")
	peersFlag := flag.String("peers", "", "comma-separated id@grpcAddr for ALL nodes, e.g. 0@raft0:9090,1@raft1:9090")
	grpcAddr := flag.String("grpc-addr", "", "gRPC listen address (default: :port from this node's -peers entry)")
	httpAddr := flag.String("http-addr", ":8080", "HTTP client API listen address")
	apiPeers := flag.String("api-peers", "", "comma-separated id@httpURL for leader redirects (optional)")
	dataDir := flag.String("data-dir", "./data", "directory for the bbolt data file")
	metricsAddr := flag.String("metrics-addr", ":2112", "Prometheus /metrics listen address")
	snapBytes := flag.Uint64("snap-bytes", 1<<20, "compact the log once it exceeds this many bytes (0 = never)")
	flag.Parse()

	grpcPeers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("raftkvd: -peers: %v", err)
	}
	if _, ok := grpcPeers[*id]; !ok {
		log.Fatalf("raftkvd: -id %d is not present in -peers", *id)
	}
	ids := sortedKeys(grpcPeers)

	listen := *grpcAddr
	if listen == "" {
		listen = ":" + portOf(grpcPeers[*id])
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("raftkvd: data dir: %v", err)
	}
	persister, err := boltstore.Open(filepath.Join(*dataDir, fmt.Sprintf("raft-%d.db", *id)))
	if err != nil {
		log.Fatalf("raftkvd: open store: %v", err)
	}
	defer persister.Close()

	applyCh := make(chan raft.ApplyMsg, 1024)
	rf := raft.New(raft.Config{
		ID:        *id,
		Peers:     ids,
		Transport: grpctransport.NewTransport(*id, grpcPeers),
		Persister: persister,
		ApplyCh:   applyCh,
		Seed:      time.Now().UnixNano(),
	})

	grpcSrv := grpctransport.NewServer(rf)
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("raftkvd: listen %s: %v", listen, err)
	}
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("raftkvd: gRPC serve stopped: %v", err)
		}
	}()

	store := kv.New()
	apiSrv := api.NewServer(*id, rf, store, applyCh, *snapBytes)

	metrics := observability.New()
	httpSrv := &http.Server{Addr: *httpAddr, Handler: metrics.InstrumentHTTP(api.NewHTTPHandler(apiSrv, parseAPIPeers(*apiPeers)))}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("raftkvd: HTTP serve stopped: %v", err)
		}
	}()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsSrv := &http.Server{Addr: *metricsAddr, Handler: metricsMux}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("raftkvd: metrics serve stopped: %v", err)
		}
	}()

	rf.Start()

	// Refresh the Raft-state gauges once a second.
	stopMetrics := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				metrics.SetRaftStats(rf.Stats())
			case <-stopMetrics:
				return
			}
		}
	}()

	log.Printf("raftkvd %s: node %d up — gRPC %s, HTTP %s, metrics %s, data %s, %d peers",
		version, *id, listen, *httpAddr, *metricsAddr, *dataDir, len(ids))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("raftkvd: shutting down")
	close(stopMetrics)
	_ = httpSrv.Close()
	_ = metricsSrv.Close()
	rf.Kill()
	grpcSrv.Stop()
	apiSrv.Close()
}

// parsePeers parses "0@host:9090,1@host2:9090" into {0:"host:9090", 1:"host2:9090"}.
func parsePeers(s string) (map[int]string, error) {
	out := make(map[int]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		at := strings.IndexByte(part, '@')
		if at < 0 {
			return nil, fmt.Errorf("peer %q missing '@'", part)
		}
		id, err := strconv.Atoi(part[:at])
		if err != nil {
			return nil, fmt.Errorf("peer %q: bad id: %w", part, err)
		}
		out[id] = part[at+1:]
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no peers given")
	}
	return out, nil
}

// parseAPIPeers parses "0@http://host:8080,..." for leader-redirect URLs.
func parseAPIPeers(s string) map[int]string {
	out := make(map[int]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if at := strings.IndexByte(part, '@'); at > 0 {
			if id, err := strconv.Atoi(part[:at]); err == nil {
				out[id] = part[at+1:]
			}
		}
	}
	return out
}

func sortedKeys(m map[int]string) []int {
	ids := make([]int, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Ints(ids)
	return ids
}

// portOf returns the port from a "host:port" address.
func portOf(addr string) string {
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		return addr[i+1:]
	}
	return addr
}
