// Package telemetry defines the router's Prometheus metrics and OpenTelemetry
// tracing setup.
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/yousufmohi/relay/router/internal/pool"
)

// Metrics groups every router metric.
type Metrics struct {
	Registry *prometheus.Registry

	Requests       *prometheus.CounterVec   // policy, status
	TTFT           *prometheus.HistogramVec // policy
	ITL            *prometheus.HistogramVec // policy
	MatchBlocks    *prometheus.HistogramVec // policy
	PromptTokens   *prometheus.CounterVec   // policy
	CachedTokens   *prometheus.CounterVec   // policy
	Rejected       *prometheus.CounterVec   // reason
	Failovers      prometheus.Counter
	FailoverPause  prometheus.Histogram
	RouteExcluded  prometheus.Counter // bounded-load exclusions
	QueueDepth     prometheus.GaugeFunc
	AdmissionSlots prometheus.GaugeFunc
}

var latencyBuckets = []float64{.005, .01, .025, .05, .075, .1, .15, .2, .3, .4, .5, .75, 1, 1.5, 2, 3, 4, 6, 8, 12, 16, 24, 32, 60}

// NewMetrics registers all metrics. queueDepth / inflight are read at scrape
// time; p supplies per-worker gauges.
func NewMetrics(p *pool.Pool, queueDepth, inflight func() float64) *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) prometheus.Collector { r.MustRegister(c); return c }
	m := &Metrics{Registry: r}
	m.Requests = f(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_requests_total", Help: "Requests by routing policy and final status."}, []string{"policy", "status"})).(*prometheus.CounterVec)
	m.TTFT = f(prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relay_ttft_seconds", Help: "Time to first token seen by the router.", Buckets: latencyBuckets}, []string{"policy"})).(*prometheus.HistogramVec)
	m.ITL = f(prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relay_itl_seconds", Help: "Inter-token latency.", Buckets: []float64{.005, .01, .02, .03, .05, .075, .1, .15, .2, .3, .5, 1, 2, 4}}, []string{"policy"})).(*prometheus.HistogramVec)
	m.MatchBlocks = f(prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "relay_route_match_blocks", Help: "Prefix blocks believed cached on the chosen worker.", Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128}}, []string{"policy"})).(*prometheus.HistogramVec)
	m.PromptTokens = f(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_prompt_tokens_total", Help: "Prompt tokens reported by workers."}, []string{"policy"})).(*prometheus.CounterVec)
	m.CachedTokens = f(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_cached_tokens_total", Help: "Prompt tokens served from worker prefix caches."}, []string{"policy"})).(*prometheus.CounterVec)
	m.Rejected = f(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_rejected_total", Help: "Requests rejected by admission control (429/503)."}, []string{"reason"})).(*prometheus.CounterVec)
	m.Failovers = f(prometheus.NewCounter(prometheus.CounterOpts{Name: "relay_failovers_total", Help: "Mid-stream failovers."})).(prometheus.Counter)
	m.FailoverPause = f(prometheus.NewHistogram(prometheus.HistogramOpts{Name: "relay_failover_pause_seconds", Help: "Client-visible gap caused by a failover.", Buckets: latencyBuckets})).(prometheus.Histogram)
	m.RouteExcluded = f(prometheus.NewCounter(prometheus.CounterOpts{Name: "relay_route_bounded_load_exclusions_total", Help: "Workers excluded from a decision by the bounded-load rule."})).(prometheus.Counter)
	m.QueueDepth = f(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "relay_queue_depth", Help: "Requests waiting in the admission queue."}, queueDepth)).(prometheus.GaugeFunc)
	m.AdmissionSlots = f(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "relay_inflight_requests", Help: "Requests dispatched to workers."}, inflight)).(prometheus.GaugeFunc)
	r.MustRegister(&workerCollector{p: p})
	return m
}

// workerCollector exports the pool's per-worker view at scrape time.
type workerCollector struct{ p *pool.Pool }

var (
	descQueue    = prometheus.NewDesc("relay_worker_queue_depth", "Waiting requests at the worker (sidecar).", []string{"worker"}, nil)
	descRunning  = prometheus.NewDesc("relay_worker_running", "Running requests at the worker (sidecar).", []string{"worker"}, nil)
	descInflight = prometheus.NewDesc("relay_worker_inflight", "This router's in-flight requests on the worker.", []string{"worker"}, nil)
	descKV       = prometheus.NewDesc("relay_worker_kv_cache_usage", "KV cache usage 0..1 (sidecar).", []string{"worker"}, nil)
	descHits     = prometheus.NewDesc("relay_worker_prefix_cache_hit_tokens_total", "Worker-reported prefix cache hit tokens.", []string{"worker"}, nil)
	descQueries  = prometheus.NewDesc("relay_worker_prefix_cache_query_tokens_total", "Worker-reported prefix cache query tokens.", []string{"worker"}, nil)
	descBreaker  = prometheus.NewDesc("relay_circuit_state", "Circuit breaker state: 0 closed, 1 open, 2 half-open.", []string{"worker"}, nil)
	descHealthy  = prometheus.NewDesc("relay_worker_eligible", "1 if the worker can receive new requests.", []string{"worker"}, nil)
	descIndex    = prometheus.NewDesc("relay_prefix_index_blocks", "Blocks in the router's prefix index for the worker.", []string{"worker"}, nil)
	descWorkers  = prometheus.NewDesc("relay_workers", "Known workers.", nil, nil)
)

func (c *workerCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descQueue, descRunning, descInflight, descKV, descHits, descQueries, descBreaker, descHealthy, descIndex, descWorkers} {
		ch <- d
	}
}

func (c *workerCollector) Collect(ch chan<- prometheus.Metric) {
	views := c.p.Snapshot()
	eligible := map[string]bool{}
	for _, v := range c.p.Eligible(nil) {
		eligible[v.ID] = true
	}
	ch <- prometheus.MustNewConstMetric(descWorkers, prometheus.GaugeValue, float64(len(views)))
	for _, v := range views {
		g := func(d *prometheus.Desc, val float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, val, v.ID)
		}
		g(descQueue, float64(v.Waiting))
		g(descRunning, float64(v.Running))
		g(descInflight, float64(v.InFlight))
		g(descKV, v.KVUsage)
		ch <- prometheus.MustNewConstMetric(descHits, prometheus.CounterValue, float64(v.PrefixHits), v.ID)
		ch <- prometheus.MustNewConstMetric(descQueries, prometheus.CounterValue, float64(v.PrefixQuery), v.ID)
		state := map[string]float64{"closed": 0, "open": 1, "half_open": 2}[v.Breaker]
		g(descBreaker, state)
		e := 0.0
		if eligible[v.ID] {
			e = 1
		}
		g(descHealthy, e)
		g(descIndex, float64(v.IndexBlocks))
	}
}
