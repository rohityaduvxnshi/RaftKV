# HTTP API

RaftKV's client-facing API is plain HTTP, served by every node (default
`-http-addr :8080`, see [operations.md](operations.md)). Writes are replicated
through the Raft log; reads are linearized via ReadIndex. The handler lives in
`internal/api/http.go` (`NewHTTPHandler`); request semantics in
`internal/api/api.go` and `internal/kv/kv.go`.

Back to the [README](../README.md). Consistency mechanics are detailed in
[raft.md](raft.md); how these guarantees are tested in
[testing.md](testing.md).

## Talking to a cluster

Any node accepts any request. Only the leader can serve it, so a non-leader
responds according to what it knows:

- **Leader known and mapped in `-api-peers`**: `307 Temporary Redirect` with
  `Location: <leader base URL><original request URI>`. The method and body are
  preserved by 307 semantics.
- **Leader unknown, or no `-api-peers` mapping for it**: `503 Service
  Unavailable`, body `not leader; leader unknown`.

Either way the response carries an `X-Leader-Id` header with the numeric node
ID of the last-known leader (`-1` if unknown). `-api-peers` is the optional
`id@httpURL` list passed to `raftkvd` (see [operations.md](operations.md));
without it, nodes never redirect — clients get 503 and must probe other nodes
themselves.

Every request is bounded by a server-side 3 second timeout
(`requestTimeout` in `internal/api/http.go`); expiry maps to `504`.

## Endpoints

Keys are single URL path segments (percent-encode anything else). All JSON
responses are `{"field": value}` objects, one per line.

| Method | Path | Request | Success | Errors |
|---|---|---|---|---|
| `GET` | `/kv/{key}` | — | `200` `{"value":"..."}` | `404` key absent; `307`/`503` not leader; `504` timeout |
| `PUT` | `/kv/{key}` | body = raw value | `204 No Content` | `307`/`503`; `504` |
| `DELETE` | `/kv/{key}` | — | `204 No Content` | `404` key absent; `307`/`503`; `504` |
| `POST` | `/kv/{key}/cas?expected=E&value=V` | query params | `200` `{"swapped":true\|false}` | `307`/`503`; `504` |
| `POST` | `/kv/{key}/append` | body = suffix | `200` `{"value":"<full value after append>"}` | `307`/`503`; `504` |

Operation semantics (from `internal/kv/kv.go`):

- **PUT** sets the key unconditionally.
- **DELETE** removes the key; `404` means it did not exist (the delete is
  still a committed log entry).
- **CAS** swaps only if the key **exists** and its current value equals
  `expected` (etcd-style: CAS on an absent key returns `"swapped": false`,
  it does not create the key). The `200` response reports the outcome; a
  failed compare is not an HTTP error.
- **append** concatenates the body onto the current value (creating the key
  if absent) and returns the resulting value. Append is deliberately
  non-idempotent — it is what makes duplicate application observable, and
  what the exactly-once tests exercise.

## Session headers: exactly-once writes

Mutating requests (PUT, DELETE, cas, append) may carry a client session:

| Header | Type | Meaning |
|---|---|---|
| `X-Client-Id` | non-empty string | Identifies the client session. Presence enables dedup. |
| `X-Seq-No` | unsigned 64-bit decimal | Monotonically increasing per client, one per logical request. |

The state machine remembers, per client, the last sequence number applied and
its result. A committed command whose `SeqNo` is less than or equal to the
last applied one is **not re-applied**; the cached result is returned. This
makes retries safe: if a leader commits your write but crashes before
replying, retrying the same request with the **same** `X-Seq-No` (against the
new leader) returns success without mutating state twice. Verified by
`TestExactlyOnceRetry` and `TestZeroSeqDedup`, and under leader changes by
`TestLinearizability`, whose clients retry writes with the same `X-Seq-No`
across re-elections.

Contract details:

- **Dedup is gated on a non-empty `X-Client-Id` alone.** `X-Seq-No: 0` is a
  valid first sequence number (`TestZeroSeqDedup` guards this).
- **Omit `X-Client-Id` and there is no dedup**: a retried write can apply
  twice. Fine for idempotent PUTs; wrong for append and CAS.
- **One outstanding request per client at a time.** The session table keeps
  only the latest `(SeqNo, result)` per client, so concurrent requests on one
  `ClientID` can return each other's cached results. Use distinct client IDs
  for concurrent request streams.
- A missing or unparsable `X-Seq-No` is treated as `0`.
- Sessions are part of the state machine snapshot, so exactly-once survives
  log compaction and restarts.

`GET` ignores the session headers; reads need no dedup.

## Consistency guarantees

- **Writes are linearizable.** Every mutation is a Raft log entry; the
  response is sent only after the entry commits and applies on the serving
  leader. If a new leader overwrites the proposal (the entry that commits at
  that log index carries a different term), the client gets a not-leader
  response and can retry safely under its session.
- **Reads are linearizable, never stale.** `GET` obtains a ReadIndex: the
  leader captures its commit index (only valid once it has committed an entry
  in its own term — the election no-op barrier) and confirms leadership with a
  heartbeat round to a quorum before serving. A leader partitioned into a
  minority cannot confirm the quorum, so the read fails with a not-leader
  response (`307` redirect, or `503` if the leader is unmapped/unknown) rather
  than returning a stale `200`. Verified by `TestNoStaleRead` (in-process) and
  `chaos/partition.sh` (live cluster: the isolated node's read status is
  asserted to be anything but `200`).
- **Linearizability of the whole API** is checked with Porcupine: 210
  concurrent operations across 3 keys, mixed appends and reads, verified
  linearizable in 3 runs under `-race` (`TestLinearizability`,
  `internal/api/linearizability_test.go`). See [testing.md](testing.md).

## Status codes

| Code | When |
|---|---|
| `200 OK` | GET hit; cas and append results (JSON body) |
| `204 No Content` | PUT committed; DELETE committed and key existed |
| `307 Temporary Redirect` | Not the leader; `Location` points at the leader's URL from `-api-peers` |
| `404 Not Found` | GET or DELETE on an absent key |
| `500 Internal Server Error` | Unexpected server error |
| `503 Service Unavailable` | Not the leader and no redirect target (leader unknown or unmapped) |
| `504 Gateway Timeout` | Entry did not commit/apply within the 3 s request timeout (e.g. no quorum) |

`X-Leader-Id` accompanies every `307` and every not-leader `503`.

## curl cookbook

Against the 5-node compose cluster ([operations.md](operations.md)), node HTTP
APIs are published on host ports 8080–8084.

```sh
# Write (unconditional). 204 on commit.
curl -i -X PUT http://localhost:8080/kv/color -d blue

# Linearizable read.
curl http://localhost:8080/kv/color
# {"value":"blue"}

# Delete. 204 if it existed, 404 if not.
curl -i -X DELETE http://localhost:8080/kv/color

# Compare-and-swap: only succeeds if the current value is "blue".
curl -X PUT http://localhost:8080/kv/color -d blue
curl -X POST 'http://localhost:8080/kv/color/cas?expected=blue&value=green'
# {"swapped":true}
curl -X POST 'http://localhost:8080/kv/color/cas?expected=blue&value=red'
# {"swapped":false}   (value is now "green")

# Append: returns the full resulting value.
curl -X POST http://localhost:8080/kv/log/append -d 'a'
# {"value":"a"}
curl -X POST http://localhost:8080/kv/log/append -d 'b'
# {"value":"ab"}
```

Exactly-once retry — the same `(client, seq)` sent twice applies once:

```sh
curl -X POST http://localhost:8080/kv/log/append -d 'x' \
     -H 'X-Client-Id: demo' -H 'X-Seq-No: 1'
# {"value":"abx"}

# Retry (same session, same seq): NOT re-applied; cached result returned.
curl -X POST http://localhost:8080/kv/log/append -d 'x' \
     -H 'X-Client-Id: demo' -H 'X-Seq-No: 1'
# {"value":"abx"}   (not "abxx")
```

Following redirects: `curl -L` re-issues the request at the leader, and 307
preserves the method and body.

```sh
curl -iL -X PUT http://localhost:8081/kv/color -d cyan
# If node 1 is a follower: first a 307 with Location + X-Leader-Id,
# then the leader's 204.
```

Caveat for the shipped compose files: `-api-peers` maps to the in-network
hostnames (`http://raft0:8080`, ...), so a `Location` received on the Docker
host will not resolve there. Host-side clients should instead probe nodes for
the leader — `chaos/lib.sh` does exactly this by treating a `200`/`404` probe
response as "leader" and `307` as "follower". Point `-api-peers` at
client-resolvable URLs if clients should follow redirects directly.

## Writing a client

1. **Pick any node** and send the request. Follow a `307` to the leader (or
   remember the leader and send there directly; `X-Leader-Id` names it).
2. **On `503` or connection failure**, retry against the other nodes with
   backoff — during an election there is briefly no leader anywhere.
3. **On `504`**, the write's fate is unknown (it may yet commit). Retry it —
   with a session, the retry is safe.
4. **Use one `X-Client-Id` per client instance** (a UUID works) and a
   **monotonically increasing `X-Seq-No`**, incremented once per logical
   request. Keep at most one request outstanding per client ID.
5. **Retry with the SAME `X-Seq-No`.** A new sequence number makes the retry a
   new request; the same one makes it a deduplicated retry. Increment only
   after a definitive response.
6. Reads carry no session state; retry them freely against any node.
