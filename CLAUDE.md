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

**Current phase.** Phase 4 complete (snapshotting / log compaction). Next: Phase 5
(client API & linearizable reads).

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

### v0.5 — Phase 4: snapshotting / log compaction (2026-07-04)

**Added:**
- Snapshot boundary offset in `internal/raft`: `log[0]` is a boundary sentinel
  whose `Index` is the last snapshotted index (`base()`); `log[i].Index ==
  base()+i`, and absolute index `a` maps to slice position `a-base()`. With no
  snapshot, `base()==0` and the code is behaviorally identical to Phase 3
  (verified — all prior tests stayed green through the refactor).
- `Raft.Snapshot(index, data)` (app-triggered compaction: save snapshot +
  `TruncatePrefix`), `Raft.LogSize()`, the `InstallSnapshot` RPC (leader sends it
  when `nextIndex[peer] <= base()`; follower installs, keeping a matching suffix
  or discarding the log), and snapshot delivery to the state machine via
  `ApplyMsg{SnapshotValid,...}`. `New` restores from a persisted snapshot.
- `kv.Store` gains `Snapshot`/`Restore`/`Dump`.
- Harness: per-node state machines rebuilt from the apply channel,
  threshold-triggered snapshotting, and `checkStoresAgree` (State Machine Safety
  across command + snapshot applies).

**Acceptance result (Windows dev box, Go 1.26.4 + mingw-w64 for -race):**
- Log stays **bounded** under sustained writes (`TestSnapshotBoundsLog`).
- A follower whose needed prefix is compacted is caught up via **InstallSnapshot**
  (`TestInstallSnapshotCatchup`).
- **Restart-from-snapshot** rebuilds the state machine (`TestRestartFromSnapshot`).
- `go test -race ./...` green; **2× repeat, no flakiness**; `vet` + `gofmt`
  clean. 3-lens adversarial review found a real crash-safety bug (torn-snapshot
  recovery), now fixed + regression-tested (see §3).

### v0.4 — Phase 3: persistence & crash recovery (2026-07-04)

**Added:**
- `internal/storage/bolt`: a bbolt-backed `raft.Persister` (buckets for meta /
  log / snap; big-endian index keys; gob values). bbolt fsyncs on every commit,
  so `SaveHardState`/`AppendEntries`/`TruncateSuffix` sit on the durability
  critical path.
- Raft core wiring: `Submit` and `HandleAppendEntries` persist log mutations
  before acting/acking; `New` reloads term+vote+log via `Persister.Load`
  (recovered entries load after the sentinel, keeping `log[i].Index == i`).
- Harness: pluggable persister factory (in-mem or bbolt), `crashAndRestart`
  (kill + reopen from disk), and `crashAllAndRestart` (whole-cluster outage).
- Tests: direct Persister round-trip + truncation; follower / leader /
  whole-cluster / single-node crash recovery.

**Acceptance result (Windows dev box, Go 1.26.4 + mingw-w64 for -race):**
- A killed node restarts, recovers its persisted log from disk, and rejoins
  without violating any invariant; a subsequent write commits on all nodes.
- Whole-cluster kill + restart: committed data survives entirely from disk and
  every node replays it (`TestWholeClusterRestart`).
- `go test -race ./...` green; **2× repeat, no flakiness**; `vet` + `gofmt`
  clean. 3-lens adversarial review found **no defects**.

### v0.3 — Phase 2: log replication + KV state machine (2026-07-04)

**Added:**
- Replication in `internal/raft`: `Submit` (client append), full
  `HandleAppendEntries` (log-matching check, truncate-only-on-genuine-conflict
  splice, fast conflict backup via `ConflictTerm`/`ConflictIndex`),
  `maybeAdvanceCommit` (quorum + §5.4.2 own-term rule), `nextIndex`/`matchIndex`,
  and an ordered `applier` loop (sync.Cond) delivering committed entries to an
  apply channel. Log now carries a sentinel at index 0 so `log[i].Index == i`.
- `internal/kv`: the replicated KV state machine (`Get`/`Put`/`Delete`/`CAS`,
  gob-encoded ops). CAS requires the key to exist (etcd-style; documented).
- Harness upgrades (`test/harness_test.go`): per-node apply drains that assert
  **State Machine Safety** (no two nodes apply a different command at one index)
  and in-order, gap-free apply; MIT-style `one()`/`nCommitted()`.
- Tests (`test/replication_test.go`): basic agreement, agreement with a follower
  down, leader-change keeps committed, deposed-leader entries overwritten,
  concurrent writers, KV op semantics, single-node.

**Acceptance result (Windows dev box, Go 1.26.4 + mingw-w64 for -race):**
- Writes replicate to a majority and apply in the **same order on every node**
  (Log Matching + State Machine Safety asserted in the drain).
- Leader change mid-workload loses no committed entry; a deposed leader's
  **uncommitted entries are overwritten** on rejoin.
- Concurrent writers stay consistent; `Put`/`CAS`/`Delete` deterministic.
- `go test -race ./...` green; **2× repeat, no flakiness**; `vet` + `gofmt`
  clean. 4-lens adversarial review (see §3).

### v0.2 — Phase 1: leader election (2026-07-04)

**Added:**
- Raft core (`internal/raft/raft.go`, `rpc.go`): Follower/Candidate/Leader state
  machine, persistent term+vote (`SaveHardState` on every change), randomized
  150–300 ms election timeouts, `RequestVote` (with the §5.4.1 up-to-date check,
  ready for real logs), heartbeats via empty `AppendEntries`. A single
  background loop handles both election timeouts and leader heartbeats (avoids
  dynamic `WaitGroup.Add` races). Figure-2 "step down on higher term" applied at
  every term observation, inbound and on RPC replies.
- Seeded fault model in `internal/transport/inmem`: ~10% drop + 0–27 ms delay
  (→ reorder) + partition (`SetConnected`), plus gob-cloning of every RPC to
  model wire serialization (no sender/receiver memory sharing).
- Test harness (`test/harness_test.go`) + election tests (`test/election_test.go`):
  `checkOneLeader` asserts **Election Safety** (no two leaders per term),
  `checkTerms` asserts term agreement, plus stability/rejoin guards.

**Acceptance result (Windows dev box, Go 1.26.4 + mingw-w64 for -race):**
- Exactly one leader at **N=3** (`TestInitialElection`) and **N=5**
  (`TestElection5Nodes`).
- Leader isolated → new leader in a **higher term**; old leader rejoining does
  not create a second leader; a **minority cannot elect**; quorum restored →
  elects again (`TestReElection`). Election Safety asserted throughout.
- **Single stable leader under a lossy network** (`TestElectionUnreliable`,
  5 nodes, ~10% drop + delays), bounded term growth over a 900 ms window.
- Term propagation (`TestTermAgreement`); clean partition-heal
  (`TestNoChurnOnRejoin`).
- `go test -race ./...` green; **5× repeat, no flakiness**; `go vet` + `gofmt`
  clean. Reviewed by a 4-lens adversarial workflow (see §3).

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
- CI: confirmed green on first push (both `main` and the `v0.1` tag).

---

## 3. Changes (running changelog, incl. reversals & what didn't work)

- **2026-07-04 — Phase 4 review caught a torn-snapshot crash-safety bug.** All 3
  review lenses independently flagged the same real defect: `Snapshot()` and
  `HandleInstallSnapshot()` persist via `SaveSnapshot` then a *separate*
  `TruncatePrefix`/`TruncateSuffix` transaction (correctly snapshot-before-
  truncate, so no data loss), but a crash *between* the two leaves the snapshot
  plus the un-truncated log prefix on disk. `New()` then appended every persisted
  entry verbatim, producing `log = [{S},{1},...,{N}]` — breaking the
  `log[i].Index == base()+i` invariant and silently corrupting apply/replication
  (no panic). Tests missed it because `crashAndRestart` only kills at clean points
  and MemPersister reuses its object across "restart". **Fixed** in `New()` by
  keeping only a contiguous suffix from `base()+1` (dropping snapshot-covered
  entries, stopping at any gap) — robust to a crash in any of the three
  save→truncate windows without needing an atomic multi-key transaction. Added a
  white-box regression test (`recover_test.go`) that builds the exact torn
  on-disk state. This is the durability lesson of the whole project: correct
  *ordering* isn't enough when the *reload* path must tolerate the torn state
  that ordering deliberately permits.
- **2026-07-04 — Phase 3 review clean; persist-ordering is the crux.** The 3-lens
  adversarial review (crash-consistency, bbolt store, recovery/concurrency)
  found **no defects**. The property that makes separate-transaction persistence
  crash-safe without atomic multi-key writes: `currentTerm` is always fsync'd
  (in `stepDownIfBehind`/`startElection`) *before* any log entry of that term is
  persisted, so a crash between the two transactions can only lose the log
  append (harmless — the leader resends), never leave `currentTerm` below a log
  entry's term. **Known trade-off (not a bug):** persist calls run while holding
  `r.mu`, so a bbolt fsync briefly blocks the node's other handlers — correct,
  and the price of durability on the critical path; batching/pipelining is a
  future perf lever, not a correctness need.
- **2026-07-04 — Adversarial review of Phase 2 + a single-node gap.** The 4-lens
  review confirmed 1 real finding (N=1 clusters never advanced `commitIndex`,
  since only a follower's reply drove `maybeAdvanceCommit`) and correctly
  rejected 3 (a `LeaderCommit` bound that coincides with Figure 2 in Phase 2, a
  defensible CAS-on-absent contract, a benign `Fatalf`-from-goroutine). Writing
  the N=1 test surfaced an **even more basic** gap the review missed: a
  single-node cluster never *won* its election either, because the majority
  check lived only inside per-peer vote-reply goroutines (none exist at N=1).
  **Fixed both** (immediate self-win in `startElection`; `maybeAdvanceCommit` in
  `Submit`). Also proactively made the follower `LeaderCommit` bound
  Figure-2-exact (min with the last *new* entry's index, not our last index) to
  kill a latent trap before entry-batching lands, and made the `one()` helper
  goroutine-safe (`Errorf` not `Fatalf`).
- **2026-07-04 — Adversarial review of Phase 1 found a real timer-reset bug.**
  A 4-lens review workflow (Raft correctness / concurrency / liveness / test
  rigor, each finding independently verified) flagged that `stepDownIfBehind`
  converted to follower **without resetting the election timer**. A leader's
  `electionDeadline` is always stale (the leader loop never refreshes it), so a
  leader learning of a higher term via an RPC *reply* would re-campaign on the
  next ~12 ms tick. **Fixed** by resetting the timer inside `stepDownIfBehind`
  (the shared choke point). **Honest caveat:** the bug does **not** manifest in
  any Phase-1 black-box test — the rejoining higher-term node's inbound
  `RequestVote` almost always resets the timer first, masking the reply path
  (verified empirically: term growth was 0 with and without the fix). The fix is
  still correct Raft and matters once later phases add real logs/commit. A
  razor-sharp deterministic reproduction would need a virtual clock (deferred).
  The review also added two missing tests (term agreement, stability under loss)
  and correctly rejected three out-of-scope/nit findings.
- **2026-07-04 — Phase 0 CI confirmed green.** First push ran the CI workflow on
  both `main` and the `v0.1` tag → both `conclusion: success` (closes the open
  Phase-0 acceptance item).

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

- **Next:** Phase 5 — client API & linearizable reads. HTTP KV API with leader
  redirect/hint; client sessions (`clientID`+`seqNo`) for exactly-once retries
  (dedup in the KV state machine); linearizable reads via ReadIndex or a leader
  lease (not stale local reads). A no-op-on-election barrier likely lands here
  (also resolves the §5.4.2 immediate re-commit note below).
- **§5.4.2 note (for Phase 5):** after a restart/new election, recovered
  committed entries re-apply only once a current-term entry commits. Tests cover
  this with a post-restart write. A no-op-on-election (or the ReadIndex barrier)
  would make re-application immediate — deferred to Phase 5 (also needed for
  linearizable reads).
- **Deferred (noted, not blocking):** outbound RPC goroutines are fire-and-forget
  with `context.Background()`; persist-under-lock serializes the node during
  fsync (perf, not correctness). A virtual clock is also deferred.
- CI green through Phase 2. Race detector requires the 64-bit `CC` on Windows
  (§4).
