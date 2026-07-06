# RaftKV — build/test entry points.
# Core targets (build/test/race/vet) are pure `go` invocations so they run
# identically on Linux CI and a Windows dev box. Prefer these over remembering
# raw flags. `race` is the gate that matters: a data race is a bug, not a warning.

GO      ?= go
# Default to "dev"; CI and the Docker build pass the real git version, e.g.
#   make build VERSION=$(git describe --tags --always --dirty)
# Kept out of $(shell ...) so the Makefile stays clean on Windows (no /dev/null).
VERSION ?= dev
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build test race vet lint fmt tidy proto clean

## build: compile every package and command
build:
	$(GO) build $(LDFLAGS) ./...

## test: run all tests
test:
	$(GO) test ./...

## race: run all tests under the race detector (the merge gate)
race:
	$(GO) test -race ./...

## vet: static analysis
vet:
	$(GO) vet ./...

## fmt: format all Go source
fmt:
	$(GO) fmt ./...

## lint: vet + report any unformatted files
lint: vet
	gofmt -l .

## tidy: sync go.mod/go.sum
tidy:
	$(GO) mod tidy

## proto: regenerate gRPC/protobuf Go code (needs protoc + protoc-gen-go[-grpc])
proto:
	protoc -I=internal/transport/grpc/proto \
	  --go_out=internal/transport/grpc/proto --go_opt=paths=source_relative \
	  --go-grpc_out=internal/transport/grpc/proto --go-grpc_opt=paths=source_relative \
	  internal/transport/grpc/proto/raft.proto

## clean: remove build/test caches
clean:
	$(GO) clean ./...
