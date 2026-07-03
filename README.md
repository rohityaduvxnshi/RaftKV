# RaftKV

A replicated, strongly-consistent key-value store — a mini-etcd — with the
**Raft consensus protocol implemented from scratch in Go**. Leader election, log
replication, crash-safe persistence, snapshotting, linearizable reads, and
idempotent client sessions. No external consensus libraries.

> **Status:** Phase 0 (scaffolding) complete. Building phase by phase — see
> [`CLAUDE.md`](CLAUDE.md) for design decisions, version history, and the plan.

## Why

Raft ([Ongaro & Ousterhout](https://raft.github.io/raft.pdf)) is *the*
understandable consensus algorithm. This implements it faithfully (Figure 2) and
proves it: the same core runs under a deterministic, seedable in-process network
that can drop/reorder/delay/partition messages *and* on a real gRPC cluster. The
headline claim is correctness under faults — **passes drop/reorder/partition +
crash tests under `go test -race`** — not throughput.

## Architecture

The Raft core (`internal/raft`) depends only on two interfaces:

- **`Transport`** — `SendRequestVote` / `SendAppendEntries` / `SendInstallSnapshot`.
  Implementations: `inmem` (tests, seeded faults) and gRPC (deployment).
- **`Persister`** — durable hard state, log (incremental append/truncate), and
  snapshot. Implementations: `MemPersister` (tests) and bbolt (deployment).

## Quick start

Requires **Go 1.26+**. The race detector additionally needs a **64-bit C
compiler** (cgo) — on Windows use mingw-w64 (see below), not the 32-bit MinGW.

```sh
make build   # compile everything
make test    # run tests
make race    # go test -race ./...  (the gate that matters)
make vet     # static analysis
```

### Windows `-race` note

`go test -race` links against the cgo race runtime, which needs a **64-bit**
`gcc`. The stock 32-bit `C:\MinGW` fails with *"64-bit mode not compiled in"*.
Install WinLibs mingw-w64 and point Go at it:

```powershell
winget install BrechtSanders.WinLibs.POSIX.UCRT
setx CC "<path>\mingw64\bin\gcc.exe"   # so cgo uses the 64-bit compiler
```

Chaos tooling (Phase 7: `tc`/`netem`, `iptables`) is **Linux-only** — run it on
Linux or WSL2.

## Layout

```
cmd/raftkvd/            server binary
internal/raft/          Raft core + Transport/Persister interfaces + RPC types
internal/transport/inmem/  simulated in-process network (tests)
test/                   unit + adversarial tests
deploy/                 docker-compose, prometheus, grafana, caddy (Phase 6+)
```

Directories are added as each phase needs them; see [`CLAUDE.md`](CLAUDE.md).

## License

TBD.
