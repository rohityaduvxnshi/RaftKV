# Contributing to RaftKV

Project overview in [README.md](README.md); design docs in
[docs/architecture.md](docs/architecture.md) and [docs/raft.md](docs/raft.md);
test strategy in [docs/testing.md](docs/testing.md). Licensed under
[MIT](LICENSE).

## Prerequisites

| Tool | Why |
|------|-----|
| Go 1.26 | Module toolchain (`go.mod` sets `go 1.26`) |
| 64-bit gcc (mingw-w64 on Windows) | `go test -race` needs cgo; 32-bit MinGW fails to link |
| `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` | Only if you change `internal/transport/grpc/proto/raft.proto` (`make proto`) |
| Docker (with compose) | The 3/5-node clusters in `deploy/` and the `chaos/` scripts |
| GNU Make | Optional; the core targets (build/test/race/vet/fmt) are plain `go` invocations you can run directly (`proto` needs `protoc`, `lint` needs `gofmt`) |

The chaos scripts are bash scripts driving the Docker CLI; run them from a
bash shell (Git Bash or WSL2 on Windows) against a running compose cluster.

## Build and test

```sh
make build        # go build -ldflags "-X main.version=$(VERSION)" ./...
make test         # go test ./...
make race         # go test -race ./...   <- the merge gate
make vet          # go vet ./...
make fmt          # go fmt ./...
make proto        # regenerate gRPC code after editing raft.proto
```

## Quality gates

All of these must pass before a PR merges. CI
(`.github/workflows/ci.yml`) enforces them on every push and pull request:

1. `gofmt -l .` reports no files.
2. `go vet ./...` is clean.
3. `go build ./...` succeeds.
4. `go test -race ./...` passes. A data race is a bug, not a warning.

## Code conventions

- Log indices are 1-based. `log[0]` is a sentinel whose `Index` is the last
  snapshotted index (`base()`), so `log[i].Index == base()+i`.
- Node and peer IDs are dense ints in `[0, N)`. `VotedFor == raft.NoVote`
  (`-1`) means "not voted this term".
- The Raft core follows the paper's Figure 2 exactly. Any deliberate
  deviation must be documented in [docs/raft.md](docs/raft.md); an
  undocumented deviation is treated as a bug.
- The core depends only on the `Transport` and `Persister` interfaces in
  `internal/raft`. Keep it that way: no transport- or storage-specific code
  in the core.

## Pull request expectations

- Behavior changes come with tests. Correctness claims in this project are
  backed by named tests; a fix without a test that failed before the fix is
  incomplete.
- No known races or flakes: run `go test -race ./...` at least twice locally.
- Changes to persistence or snapshot code must consider crash points between
  separate transactions (see the torn-snapshot entry in
  [CHANGELOG.md](CHANGELOG.md) v0.5) and the reload path that tolerates them.
- Update [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
