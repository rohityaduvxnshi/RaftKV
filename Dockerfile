# Multi-stage build for the raftkvd server. The image runs the server only
# (tests, including -race, run in CI/dev), so the binary is built CGO-free and
# fully static, then dropped into a distroless base for a tiny attack surface.
FROM golang:1.26 AS build
WORKDIR /src
# Copy manifests first for layer caching. go.sum may not exist yet (no deps).
COPY go.mod ./
COPY go.su[m] ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/raftkvd ./cmd/raftkvd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/raftkvd /raftkvd
# 8080 = client HTTP API, 9090 = gRPC inter-node transport, 2112 = /metrics.
EXPOSE 8080 9090 2112
ENTRYPOINT ["/raftkvd"]
