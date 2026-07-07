# Operations Guide

How to run, monitor, break, and load-test a RaftKV cluster. For what the
system is, see [architecture.md](architecture.md); for the client API, see
[api.md](api.md); for the test suite behind the correctness claims, see
[testing.md](testing.md). Back to the [README](../README.md).

## 1. Running a single node

`raftkvd` (see `cmd/raftkvd/main.go`) runs one node: the Raft core over the
gRPC transport, a bbolt persister, the KV state machine, and the HTTP client
API. Run N of them with a shared `-peers` list to form a cluster.

### Flag reference

| Flag | Default | Meaning |
|------|---------|---------|
| `-id` | `0` | This node's ID (dense, 0-based) |
| `-peers` | `""` (required) | Comma-separated `id@grpcAddr` for **all** nodes, e.g. `0@raft0:9090,1@raft1:9090` |
| `-grpc-addr` | `""` | gRPC listen address; defaults to `:port` taken from this node's own `-peers` entry |
| `-http-addr` | `:8080` | HTTP client API listen address |
| `-api-peers` | `""` | Comma-separated `id@httpURL` used for leader redirects (optional; without it a non-leader answers 503 instead of 307) |
| `-data-dir` | `./data` | Directory for the bbolt data file (`raft-<id>.db`); created if missing |
| `-metrics-addr` | `:2112` | Prometheus `/metrics` listen address |
| `-snap-bytes` | `1048576` | Compact the log via snapshot once it exceeds this many bytes (`0` = never) |

The node's `-id` must appear in `-peers`, or the process exits at startup.

### Single node

A one-node cluster commits immediately (quorum of one):

```sh
raftkvd -id 0 -peers 0@127.0.0.1:9090 -http-addr :8080 -data-dir ./data
```

```sh
curl -X PUT http://127.0.0.1:8080/kv/foo -d bar   # 204
curl http://127.0.0.1:8080/kv/foo                 # {"value":"bar"}
```

### Three nodes on localhost

Each node needs its own gRPC port, HTTP port, metrics port, and data
directory:

```sh
PEERS=0@127.0.0.1:9090,1@127.0.0.1:9091,2@127.0.0.1:9092
API=0@http://127.0.0.1:8080,1@http://127.0.0.1:8081,2@http://127.0.0.1:8082

raftkvd -id 0 -peers $PEERS -api-peers $API -http-addr :8080 \
        -metrics-addr :2112 -data-dir ./data0 &
raftkvd -id 1 -peers $PEERS -api-peers $API -http-addr :8081 \
        -metrics-addr :2113 -data-dir ./data1 &
raftkvd -id 2 -peers $PEERS -api-peers $API -http-addr :8082 \
        -metrics-addr :2114 -data-dir ./data2 &
```

Write to any node; a non-leader replies `307 Temporary Redirect` to the
leader's `-api-peers` URL.

## 2. Docker Compose clusters

Two compose files under `deploy/` bring up a full cluster plus Prometheus and
Grafana:

```sh
docker compose -f deploy/docker-compose.3node.yml up --build   # tolerates 1 failure
docker compose -f deploy/docker-compose.5node.yml up --build   # tolerates 2 failures
```

| Service | Host port(s) | Notes |
|---------|--------------|-------|
| `raft0`..`raft4` HTTP API | `8080`..`8084` (3-node: `8080`..`8082`) | Each maps to container port 8080; write to any node |
| Prometheus | `9091` | Scrapes every node's `:2112` every 5 s (`deploy/prometheus/prometheus.yml`, or `prometheus.3node.yml` for the 3-node file) |
| Grafana | `3000` | Anonymous admin, dashboard auto-provisioned |

Each node persists to its own named volume (`raft0-data:/data` etc.), so
committed data survives `docker compose restart` and container kills. gRPC
(port 9090) and metrics (2112) stay on the internal network; only the HTTP
APIs, Prometheus, and Grafana are published to the host.

### Distroless nonroot and volume ownership

The image (`Dockerfile`) runs as the distroless `:nonroot` user (uid 65532).
Docker creates a named volume's mountpoint root-owned, which that user cannot
write — the original symptom was all nodes crash-looping with
`bolt: open /data/raft-0.db: permission denied`. The fix ships `/data` inside
the image with `COPY --chown=65532:65532`, because Docker seeds a **fresh,
empty** volume from the image directory's ownership. This only helps fresh
volumes: if you have volumes created by an older image (or otherwise
root-owned), discard them with `down -v` before `up`.

## 3. Metrics reference

All metrics come from `internal/observability/metrics.go`. Each process hosts
one node, so metrics are unlabeled by node; Prometheus adds a `node` label
per scrape target via `relabel_configs` in `deploy/prometheus/*.yml`. The
Raft gauges are refreshed once per second from `Raft.Stats()`. The registry
also includes the standard Go runtime collector.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `raftkv_current_term` | gauge | — | Current Raft term |
| `raftkv_is_leader` | gauge | — | 1 if this node is the leader, else 0 |
| `raftkv_commit_index` | gauge | — | Highest committed log index |
| `raftkv_last_applied` | gauge | — | Highest applied log index |
| `raftkv_log_bytes` | gauge | — | Approximate on-disk log size in bytes |
| `raftkv_http_request_duration_seconds` | histogram | `method`, `code` | HTTP client-API request latency |
| `raftkv_http_requests_total` | counter | `method`, `code` | Total HTTP client-API requests |

Useful PromQL:

```promql
# Election Safety, live: exactly one leader across the cluster.
sum(raftkv_is_leader)

# Commit-index convergence: 0 when every node has caught up.
max(raftkv_commit_index) - min(raftkv_commit_index)

# Write throughput (successful requests/s).
sum(rate(raftkv_http_requests_total{code=~"2.."}[1m]))

# p99 client-API latency.
histogram_quantile(0.99,
  sum by (le) (rate(raftkv_http_request_duration_seconds_bucket[5m])))
```

## 4. Grafana

Grafana is fully provisioned by the compose files — no manual setup:

- **Auth:** anonymous access enabled with org role `Admin`, login form
  disabled (`GF_AUTH_ANONYMOUS_ENABLED`, `GF_AUTH_ANONYMOUS_ORG_ROLE`,
  `GF_AUTH_DISABLE_LOGIN_FORM` in the compose files). Open
  `http://localhost:3000` directly.
- **Datasource:** `deploy/grafana/provisioning/datasources/prometheus.yml`
  points at `http://prometheus:9090` (uid `prometheus`, default).
- **Dashboard:** the provider in
  `deploy/grafana/provisioning/dashboards/provider.yml` loads
  `deploy/grafana/dashboards/raftkv.json` — the "RaftKV Cluster" dashboard.

Panels:

| Panel | Query basis |
|-------|-------------|
| Leader (1 = leader) by node | `raftkv_is_leader` |
| Current term by node | `raftkv_current_term` |
| commitIndex / lastApplied by node | `raftkv_commit_index`, `raftkv_last_applied` |
| Write throughput (req/s, 2xx) | `sum(rate(raftkv_http_requests_total{code=~"2.."}[1m]))` |
| Request latency p50 / p99 | `histogram_quantile` over `raftkv_http_request_duration_seconds_bucket` |
| Log size (bytes) by node | `raftkv_log_bytes` |

## 5. Chaos runbook

The `chaos/` scripts use Docker primitives only (`kill`,
`network disconnect`), so they run anywhere Docker does — no Linux
`tc`/`netem`/`iptables`. Latency injection is deliberately out of scope here
(Linux-only). Both scripts are bash; on Windows run them from WSL2 or Git
Bash with Docker available.

**Prerequisites:** the 5-node cluster up
(`docker compose -f deploy/docker-compose.5node.yml up --build`), node HTTP
APIs on host ports 8080–8084, and — for `partition.sh` — the compose network
`raftkv5_default` with default container names (`raftkv5-raftN-1`).

`chaos/lib.sh` provides the shared leader probe: `find_leader` GETs
`/kv/_probe` on each node and identifies the leader by HTTP status — the
leader serves the read (200/404), a follower answers 307, a dead or isolated
node times out. `wait_leader [exclude]` polls for up to 30 s.

### `chaos/kill-leader.sh`

1. Find the leader; PUT `survive=v1` through it (seq 1).
2. `docker compose kill` the leader's container.
3. Wait for a **different** node to become leader.
4. PUT `survive=v2` (seq 2) through the new leader and read it back,
   asserting the value is `v2`.
5. Restart the killed container.

PASS proves: the cluster re-elects after losing its leader and stays
writable — availability under single-node failure.

### `chaos/partition.sh`

1. Find the leader; `docker network disconnect` it from `raftkv5_default`
   (the node keeps running but cannot reach any peer).
2. Wait for the 4-node majority to elect a new leader; PUT a unique per-run
   value (`part=v$$`, seq 1) through it and read it back.
3. Probe the **isolated** node: assert its read does **not** return 200 —
   without a quorum, ReadIndex confirmation fails, so no stale read is
   served.
4. `docker network connect` to heal; retry for up to 20 s until a leader
   serves the per-run value again.

PASS proves: the minority side is unavailable rather than stale, the
majority stays available, and the committed write survives the heal (Leader
Completeness).

### Why per-run client IDs and values

Both scripts originally shared client ID `chaos`, so the second script's
`seq 1` write was **correctly deduplicated** against the first's by the
exactly-once session table — the store working as designed, misread as a
chaos failure. Each script now sets its own `CLIENT` (`chaos-kill`;
`chaos-part-$$` per run), and `partition.sh` writes a unique per-run value
(`v$$`) so the final assertion proves *this run's* write round-tripped
through the partition and heal, not a stale value from an earlier run. The
general lesson: assert on values, not just status codes.

## 6. Load testing

`cmd/loadtest` fires concurrent PUTs (each worker its own exactly-once
session, `X-Client-Id: load-<n>` with incrementing `X-Seq-No`) and reports
throughput plus client-side latency percentiles.

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `http://127.0.0.1:8080` | Leader HTTP base URL |
| `-c` | `16` | Concurrent clients |
| `-d` | `5s` | Test duration |

**Point `-addr` at the leader.** A follower 307-redirects to `-api-peers`
URLs, which in the compose cluster are container hostnames (`http://raft1:8080`)
the host cannot resolve, so redirected requests count as failures. Find the
leader first — the node whose `/kv/_probe` returns 200/404 rather than 307:

```sh
for p in 8080 8081 8082 8083 8084; do
  echo "$p -> $(curl -s -m 3 -o /dev/null -w '%{http_code}' http://localhost:$p/kv/_probe)"
done
# or: source chaos/lib.sh && find_leader
```

```sh
go run ./cmd/loadtest -addr http://localhost:8081 -c 16 -d 5s
```

Measured at v0.8 (5-node compose cluster, Docker on the Windows dev box, 16
clients, 5 s): **486 writes/s, 0 failures, p50 32 ms, p99 53 ms, p99.9
71 ms**. The ~32 ms p50 is HTTP plus a Raft round-trip plus a **bbolt fsync
on the commit path** — the store is durability-first, not latency-optimized.
Run the load test against a **fresh** cluster (see the election-storm note
below).

## 7. Troubleshooting

**All nodes crash-loop: `bolt: open /data/raft-N.db: permission denied`.**
The named volumes are root-owned (created by an older image, or before the
`--chown` fix). The nonroot container user (uid 65532) cannot write them.
Fix: `docker compose -f deploy/docker-compose.5node.yml down -v` to discard
the volumes, then `up --build`; fresh volumes inherit the image's `/data`
ownership. Note `down -v` deletes all stored data.

**Election storm after heavy chaos (latency-spiky hosts).** Symptom:
`raftkv_current_term` climbing several times per second, `sum(raftkv_is_leader)`
stuck at 0, writes answered 503 (no node is leader, so no redirect target).
Observed on Docker-on-Windows after many back-to-back chaos cycles:
accumulated churn plus host heartbeat-latency spikes against the 150–300 ms
randomized election timeout (`internal/raft/raft.go`) produces split-vote
livelock. It survives `docker compose restart` (the persisted high term
reloads and keeps storming); the fix is a fresh start:
`down -v && up --build`, which is immediately stable at term 1. Longer-term
levers for high-latency deployments: raise the election-timeout constants,
or implement Pre-Vote. This is environmental, not a normal-operation defect
— the same core passes all deterministic in-process chaos tests and is
stable at term 1 on every fresh start.

**`go test -race` fails to link on Windows.** The race detector needs a
64-bit C compiler; a 32-bit MinGW gcc fails with "64-bit mode not compiled
in". Install mingw-w64 (e.g. WinLibs via winget) and pin Go's `CC`
environment variable to its `gcc.exe` so PATH ordering cannot select the
32-bit one.

## 8. Production notes and limitations

- **Static membership.** The peer set is fixed at startup via `-peers` and
  identical on every node. There is no joint consensus or single-server
  membership change; adding or removing a node means bringing up a new
  cluster.
- **No TLS or auth on inter-node gRPC.** The transport dials with insecure
  credentials (`internal/transport/grpc/grpc.go`); inter-node traffic is
  assumed to run on a trusted private network, as in the compose files. The
  HTTP client API is likewise plaintext and unauthenticated.
- **Single-key operations only.** `Get`/`Put`/`Delete`/`CAS`/`Append` each
  act on one key; there are no multi-key transactions. CAS requires the key
  to exist (etcd-style).
- **Durability over latency.** bbolt fsyncs on every log append; persistence
  runs on the commit critical path. Batching/pipelining is a known future
  performance lever, not a correctness need.
- **Log growth is bounded** by snapshotting once the log exceeds
  `-snap-bytes` (default 1 MiB); set it to `0` to disable compaction.
