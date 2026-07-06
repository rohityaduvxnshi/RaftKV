// Package observability exposes Prometheus metrics for a RaftKV node: Raft state
// gauges (leader/term/commit/applied/log size) plus HTTP request latency and
// throughput. Each process hosts one node, so metrics are unlabeled by node —
// Prometheus distinguishes nodes by scrape target.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rohityaduvxnshi/RaftKV/internal/raft"
)

// Metrics holds a RaftKV node's collectors on a private registry.
type Metrics struct {
	reg *prometheus.Registry

	term        prometheus.Gauge
	isLeader    prometheus.Gauge
	commitIndex prometheus.Gauge
	lastApplied prometheus.Gauge
	logBytes    prometheus.Gauge

	reqDuration *prometheus.HistogramVec
	reqTotal    *prometheus.CounterVec
}

// New builds and registers the collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	f := promauto.With(reg)
	return &Metrics{
		reg:         reg,
		term:        f.NewGauge(prometheus.GaugeOpts{Name: "raftkv_current_term", Help: "Current Raft term."}),
		isLeader:    f.NewGauge(prometheus.GaugeOpts{Name: "raftkv_is_leader", Help: "1 if this node is the leader, else 0."}),
		commitIndex: f.NewGauge(prometheus.GaugeOpts{Name: "raftkv_commit_index", Help: "Highest committed log index."}),
		lastApplied: f.NewGauge(prometheus.GaugeOpts{Name: "raftkv_last_applied", Help: "Highest applied log index."}),
		logBytes:    f.NewGauge(prometheus.GaugeOpts{Name: "raftkv_log_bytes", Help: "Approximate on-disk log size in bytes."}),
		reqDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raftkv_http_request_duration_seconds",
			Help:    "HTTP client-API request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "code"}),
		reqTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_http_requests_total",
			Help: "Total HTTP client-API requests.",
		}, []string{"method", "code"}),
	}
}

// Handler serves the metrics for Prometheus to scrape.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// SetRaftStats updates the Raft-state gauges (call periodically).
func (m *Metrics) SetRaftStats(s raft.Stats) {
	m.term.Set(float64(s.Term))
	if s.IsLeader {
		m.isLeader.Set(1)
	} else {
		m.isLeader.Set(0)
	}
	m.commitIndex.Set(float64(s.CommitIndex))
	m.lastApplied.Set(float64(s.LastApplied))
	m.logBytes.Set(float64(s.LogBytes))
}

// InstrumentHTTP wraps a handler to record request latency (p50/p99 via the
// histogram) and throughput (rate of the counter).
func (m *Metrics) InstrumentHTTP(next http.Handler) http.Handler {
	return promhttp.InstrumentHandlerCounter(m.reqTotal,
		promhttp.InstrumentHandlerDuration(m.reqDuration, next))
}
