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

## proto: regenerate gRPC/protobuf code (wired up in Phase 6)
proto:
	@echo "no .proto sources yet — added in Phase 6 (gRPC transport)"

## clean: remove build/test caches
clean:
	$(GO) clean ./...
