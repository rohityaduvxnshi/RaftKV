// Command raftkvd runs a single RaftKV server node.
//
// Phase 0: a placeholder entry point so the binary builds. Real flags
// (node ID, peer addresses, data dir, listen ports) and server wiring arrive in
// later phases as the Raft core, KV state machine, and gRPC/HTTP layers land.
package main

import "fmt"

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	fmt.Printf("raftkvd %s — scaffolding (Phase 0)\n", version)
}
