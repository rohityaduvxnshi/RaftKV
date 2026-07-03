# CLAUDE.md — RaftKV project memory

Single source of truth for RaftKV. Updated at the end of every phase and whenever
a significant decision or reversal lands. Keep it accurate to the repo, never
aspirational.

---

## 1. Context

**What/why.** RaftKV is a replicated, strongly-consistent key-value store — a
mini-etcd — with the **Raft consensus protocol implemented from scratch in Go**
(no `hashicorp/raft`, `etcd/raft`, or `dragonboat`). It provides leader election,
log replication, crash-safe persistence, snapshotting/log compaction,
linearizable reads, and idempotent client sessions. It follows Ongaro &
Ousterhout's Raft paper **Figure 2** exactly; deliberate deviations are recorded
in §3.

**Current phase.** Phase 0 complete (scaffolding + toolchain). Next: Phase 1
(leader election).

**High-level architecture (grown per phase):**

```
Client ──HTTP──> API/leader-redirect ──> Raft core ──apply──> KV state machine
                                            │
                        Transport (RPC)  ───┤───  Persister (durability)
                        ├ inmem (tests, seeded faults)      ├ MemPersister (tests)
                        └ gRPC (deployment)                 └ bbolt (deployment)
```

The **Raft core depends only on two interfaces** — `Transport` and `Persister`
(both in `internal/raft`). That split is the whole point: the same core passes
deterministic adversarial in-process tests *and* runs a real networked cluster,
just by swapping implementations.

**Key design decisions & reasoning:**

- **Go.** Goroutines/channels map cleanly onto Raft's concurrency; it's the
  language of the MIT 6.5840 labs and of etcd/hashicorp-raft.
- **Two Transports (in-mem + gRPC).** Tests need a *deterministic, seedable*
  network that can drop/reorder/delay/partition messages so failures reproduce;
  deployment needs a real one. gRPC for deployment because it gives typed
  RPCs + streaming for free.
- **Two Persisters (in-mem + bbolt).** `bbolt` (pure-Go embedded B+tree KV) is
  the WAL-style log + metadata store; fsync sits on the critical path. In-mem
  Persister lets the core be tested without disk while exercising the identical
  interface (including truncation semantics).
- **Persister interface is incremental, not blob-dump.** It exposes
  `AppendEntries` / `TruncateSuffix` / `TruncatePrefix` / `SaveSnapshot` rather
  than a single "serialize the whole state" call, because a real write-ahead log
  appends one entry at a time and must not re-serialize the entire log on every
  commit. (Provisional — may be refined when the bbolt impl lands in Phase 3;
  any change recorded in §3.)
- **HTTP for the client API; gRPC only inter-node.** The brief allows "HTTP
  and/or gRPC" for the client API. HTTP is the lazy-correct choice: trivial to
  curl/load-test/demo, no client codegen. gRPC is reserved for the Raft
  transport where typed messages matter. (ponytail)
- **Prometheus only; OpenTelemetry tracing skipped.** The brief marks OTel
  optional. Prometheus metrics + Grafana cover the observability acceptance.
- **Conventions:** log indices are **1-based** (index 0 = the zero entry before
  the first real entry / the snapshot boundary); node/peer IDs are dense ints
  `[0, N)`; `VotedFor == -1` (`raft.NoVote`) means "not voted this term".
- **Repo layout grown per phase, not scaffolded up front.** Only directories a
  phase actually populates are created (avoids empty "for later" packages).

---

## 2. Version history

Entries map 1:1 to git tags.

### v0.1 — Phase 0: scaffolding & tooling (2026-07-03)

**Added:**
- Go module `github.com/rohityaduvxnshi/RaftKV` (Go 1.26).
- Core interfaces + types in `internal/raft`: `Transport`, `RPCHandler`,
  `Persister`, and Figure-2 RPC message types (`RequestVote*`,
  `AppendEntries*` with fast-conflict-backup fields, `InstallSnapshot*`),
  `LogEntry`, `HardState`, `Snapshot`, `PersistentState`.
- `MemPersister` (in-memory Persister) and `internal/transport/inmem` (simulated
  network; reliable delivery for now, seeded fault model comes in Phase 1).
- `cmd/raftkvd` placeholder entry point.
- Smoke test (`test/smoke_test.go`): concurrent RPC round-trip + unreachable-peer
  path over the in-mem network, passing under `-race`.
- `Makefile` (build/test/race/vet/lint/fmt/tidy/proto/clean), `Dockerfile`
  (multi-stage, static CGO-free binary → distroless), `.gitignore`,
  placeholder `deploy/docker-compose.{3,5}node.yml`.
- GitHub Actions CI (`.github/workflows/ci.yml`): gofmt-check + vet + build +
  `go test -race` on ubuntu-latest, every push/PR.

**Acceptance result (measured on the dev box, see §4 for toolchain):**
- `make build` → exit 0 (clean).
- `make test` → exit 0 (`ok test`).
- `make race` (`go test -race ./...`) → exit 0. Race detector verified
  *functional* against a deliberate data-race probe before trusting it.
- `go vet ./...` → exit 0; `gofmt -l .` → clean.
- Interfaces compile; `CLAUDE.md` present with all five sections.
- CI: to be confirmed green on first push.

---

## 3. Changes (running changelog, incl. reversals & what didn't work)

- **2026-07-03 — Toolchain bootstrap on a bare Windows box.** No Go/gcc/make
  present. Installed Go 1.26.4 as a *portable zip* to
  `%LOCALAPPDATA%\Programs\go` (no admin, reversible). **What didn't work:**
  `Expand-Archive` (PS 5.1) timed out (>3 min) on the ~350 MB extract — switched
  to `System.IO.Compression.ZipFile.ExtractToDirectory` (~17 s).
- **2026-07-03 — `-race` needs a 64-bit C compiler.** The pre-existing
  `C:\MinGW` is 32-bit MinGW.org (gcc 6.3.0) and fails cgo with *"sorry,
  unimplemented: 64-bit mode not compiled in"*, so `go test -race` couldn't
  link. Installed WinLibs mingw-w64 (gcc 16.1.0, `x86_64-w64-mingw32`) via winget
  and pinned Go's `CC` to it (User env) so PATH ordering can't route back to the
  32-bit gcc. `-race` then works.
- **2026-07-03 — Makefile `VERSION`.** Dropped `$(shell git describe … 2>/dev/null
  || echo dev)` → plain `VERSION ?= dev`; the `/dev/null` redirect printed *"The
  system cannot find the path specified."* under Windows `make`. CI/Docker pass
  the real git version explicitly via `make build VERSION=…`.

---

## 4. Deployment

**Target:** static project page at **`raftkv.dash-board.in`** via the existing
Caddy per-subdomain auto-HTTPS pattern (zip → scp → `Expand-Archive`).

**VPS constraint (important):** the dash-board.in VPS is **Windows Server 2022**
(Dallas, `144.172.98.43`). RaftKV's chaos tooling (`tc`/`netem`, `iptables`) is
**Linux-only** and its cluster is docker-compose based, so **the live 5-node
cluster is NOT hosted on the Windows VPS.** Plan:
- **VPS:** serve the *static page only* (demo video, benchmark tables, Grafana
  screenshots, safety-invariant summary, GitHub link).
- **Chaos/demo video:** recorded locally on **Linux/WSL2** where netem/iptables
  work.
- **Optional live cluster:** if a live endpoint is wanted, deploy to a Linux
  host (e.g. Oracle Cloud Always Free) and link it from the page.

**Status:** not yet deployed (Phase 8). This section will be updated to state
exactly what is live vs. local when it lands.

**Local dev toolchain (Windows 11 dev box):**
- Go 1.26.4 → `%LOCALAPPDATA%\Programs\go` (User `GOROOT` + PATH).
- 64-bit gcc for `-race` → WinLibs mingw-w64 16.1.0; User `CC` pinned to its
  `gcc.exe`.
- GNU Make 4.4.1 (ezwinports) via winget.
- **Windows caveat:** `go test -race` requires the 64-bit `CC` above. Chaos
  scripts (Phase 7) do **not** run on Windows — use Linux/WSL2.

---

## 5. Known issues / next

- **Next:** Phase 1 — leader election (Follower/Candidate/Leader state machine,
  persistent term+vote, randomized 150–300 ms election timeouts, `RequestVote`,
  heartbeats via empty `AppendEntries`), plus the seeded fault model
  (drop/reorder/delay/partition) in the in-mem transport and Election-Safety
  assertions.
- CI not yet observed green (no push since scaffolding) — confirm on first push.
- `internal/transport/inmem` currently delivers synchronously/reliably; the
  adversarial fault model is added in Phase 1.
