# Changelog

All notable changes to RaftKV. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); each section maps
1:1 to a git tag. See [README.md](README.md) for the project overview.

## [v0.8](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.8) — 2026-07-06

Phase 7: chaos and correctness.

### Added
- Docker-native chaos scripts (`chaos/kill-leader.sh`, `chaos/partition.sh`):
  kill or partition the leader, assert re-election, continued writability of
  the majority, no stale reads from the isolated minority, and survival of
  committed writes across the heal (Leader Completeness).
- Porcupine linearizability check
  (`internal/api/linearizability_test.go`): 210 concurrent ops across 3 keys
  verified linearizable against a per-key register model, 3 runs under `-race`.
- `cmd/loadtest`: concurrent-PUT generator reporting throughput and
  client-side latency percentiles. Measured on a 5-node compose cluster
  (16 clients, 5 s): 486 writes/s, 0 failures, p50 32 ms, p99 53 ms,
  p99.9 71 ms, with bbolt fsync on the commit path.

### Fixed
- `partition.sh` reused the client session of `kill-leader.sh`, so its first
  write was correctly deduplicated by the exactly-once session table and the
  weak original assertion hid it. Each script now has its own session
  (`partition.sh` additionally uses a fresh session per run), and the
  assertion verifies a unique per-run value survives the partition heal.

## [v0.7](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.7) — 2026-07-06

Phase 6: gRPC transport and observability.

### Added
- gRPC transport (`internal/transport/grpc`) mirroring the Figure 2 RPCs;
  `TestGRPCReplication` runs a real 3-node cluster on localhost through the
  same election and replication checks as the in-memory transport.
- Fully wired `cmd/raftkvd`: gRPC transport, bbolt persister, KV state
  machine, HTTP API, snapshot-driven log compaction, graceful shutdown.
- Prometheus metrics (`internal/observability`) on `/metrics`, plus
  `deploy/docker-compose.{3,5}node.yml` with provisioned Prometheus and a
  Grafana dashboard. Verified: 5 containers elect exactly one leader and the
  commit index converges on all nodes.

### Fixed
- Compose crash loop: the distroless `nonroot` user (uid 65532) could not
  write the root-owned named volume. `/data` is now created in the build
  stage and copied with `--chown=65532:65532`, so a fresh volume inherits
  nonroot ownership.

## [v0.6](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.6) — 2026-07-05

Phase 5: client API, exactly-once sessions, linearizable reads.

### Added
- Linearizable reads: a no-op barrier entry committed on election, plus
  `ReadIndex` with quorum confirmation; a leader isolated in a minority
  refuses stale reads (`TestNoStaleRead`).
- Exactly-once client sessions in `internal/kv`: `(ClientID, SeqNo)`
  deduplication with cached results, included in snapshots so it survives
  compaction (`TestExactlyOnceRetry`).
- HTTP client API (`internal/api`): `GET`/`PUT`/`DELETE`/`POST cas|append`,
  `X-Client-Id`/`X-Seq-No` headers, 307 redirect from followers to the leader
  (`TestWriteToFollowerRedirects`, `TestHTTPRoundTrip`).

### Fixed
- Lost-wakeup race: `api.Server.mutate` registered its result waiter after
  `Submit` returned, so a fast commit (notably single-node) could notify
  before the waiter existed and a committed write spuriously timed out. The
  waiter is now registered under the server mutex spanning `Submit`.
- Deduplication was gated on `SeqNo != 0`, silently disabling exactly-once
  for clients that 0-index their sequence numbers. Now gated on a non-empty
  `ClientID` alone (`TestZeroSeqDedup`).

## [v0.5](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.5) — 2026-07-04

Phase 4: snapshotting and log compaction.

### Added
- Snapshot boundary sentinel: `log[0].Index` is the last snapshotted index,
  so absolute index `a` maps to slice position `a - base()`.
- `Raft.Snapshot(index, data)` for app-triggered compaction, the
  `InstallSnapshot` RPC for followers whose needed prefix is compacted
  (`TestInstallSnapshotCatchup`), and restart-from-snapshot restore
  (`TestRestartFromSnapshot`).
- `kv.Store` `Snapshot`/`Restore`/`Dump`; log stays bounded under sustained
  writes (`TestSnapshotBoundsLog`).

### Fixed
- Torn-snapshot recovery: a crash between `SaveSnapshot` and the following
  log truncation left the snapshot plus the un-truncated prefix on disk, and
  reload appended every persisted entry verbatim, silently breaking the
  `log[i].Index == base()+i` invariant. `New()` now keeps only a contiguous
  suffix from `base()+1`; regression-tested with the exact torn on-disk state
  (`internal/raft/recover_test.go`).

## [v0.4](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.4) — 2026-07-04

Phase 3: persistence and crash recovery.

### Added
- bbolt-backed `raft.Persister` (`internal/storage/bolt`): buckets for
  metadata, log, and snapshot; fsync on every commit, so persistence sits on
  the durability critical path.
- Persist-before-ack wiring in the core: `Submit` and `HandleAppendEntries`
  persist log mutations before acting; `New` reloads term, vote, and log.
- Crash-recovery tests: follower, leader, single-node, and whole-cluster
  kill-and-restart (`TestWholeClusterRestart`) with committed data surviving
  entirely from disk.

### Changed
- Crash-safety without atomic multi-key writes rests on ordering:
  `currentTerm` is always persisted before any log entry of that term, so a
  crash between the two transactions can only lose a resendable append.

## [v0.3](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.3) — 2026-07-04

Phase 2: log replication and the KV state machine.

### Added
- Full replication: `Submit`, `HandleAppendEntries` with log-matching check
  and fast conflict backup (`ConflictTerm`/`ConflictIndex`), quorum commit
  with the Section 5.4.2 own-term rule, and an ordered applier loop.
- `internal/kv` state machine: `Get`/`Put`/`Delete`/`CAS` (CAS requires the
  key to exist, etcd-style).
- Harness assertions for State Machine Safety: no two nodes apply a
  different command at the same log index; applies are in-order and gap-free.

### Fixed
- Single-node clusters never won elections (the majority check lived only in
  per-peer vote-reply goroutines, none of which exist at N=1) and never
  advanced the commit index (only follower replies drove
  `maybeAdvanceCommit`). Fixed with an immediate self-win in `startElection`
  and a `maybeAdvanceCommit` call in `Submit`.
- Follower `LeaderCommit` bound made Figure 2 exact: min with the last new
  entry's index rather than the follower's last index.

## [v0.2](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.2) — 2026-07-04

Phase 1: leader election.

### Added
- The follower/candidate/leader state machine with persistent term and vote,
  randomized 150–300 ms election timeouts, `RequestVote` with the
  Section 5.4.1 up-to-date check, and heartbeats via empty `AppendEntries`.
- Seeded fault model in the in-memory transport: ~10% drop, 0–27 ms delay
  (reordering), partitions, and gob-cloning of every RPC.
- Election tests asserting Election Safety at N=3 and N=5, re-election after
  leader isolation, no election by a minority, and a single stable leader
  under a lossy network (`TestElectionUnreliable`).

### Fixed
- Timer-reset bug found by adversarial review: `stepDownIfBehind` converted
  to follower without resetting the election timer, so a leader learning of
  a higher term via an RPC reply would re-campaign on the next tick. Fixed
  at the shared choke point. Caveat: no Phase 1 black-box test triggers it,
  because an inbound `RequestVote` usually resets the timer first.

## [v0.1](https://github.com/rohityaduvxnshi/RaftKV/releases/tag/v0.1) — 2026-07-03

Phase 0: scaffolding and tooling.

### Added
- Go module `github.com/rohityaduvxnshi/RaftKV` with the core contracts in
  `internal/raft`: `Transport`, `RPCHandler`, `Persister`, and the Figure 2
  RPC message types.
- `MemPersister`, the in-memory transport, a smoke test passing under
  `-race`, and a `cmd/raftkvd` placeholder.
- `Makefile`, multi-stage distroless `Dockerfile`, compose placeholders, and
  GitHub Actions CI (gofmt check, vet, build, `go test -race`) on every push
  and pull request — green from the first push.
