// Package pool tracks worker membership and health: which workers exist,
// which may receive traffic right now, and the live load / cache numbers
// their sidecars stream to us.
package pool

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/yousufmohi/relay/gen/relay/v1"
	"github.com/yousufmohi/relay/router/internal/prefix"
)

// Endpoint is one worker as discovered: the model server's HTTP base URL and
// the sidecar's gRPC address (empty = no sidecar, e.g. the M0 pass-through).
type Endpoint struct {
	ID          string
	URL         string
	SidecarAddr string
}

// Config tunes health detection.
type Config struct {
	RouterID         string
	BlockSize        int           // tokens per prefix block (to size indexes)
	HealthInterval   time.Duration // active health check period
	HealthTimeout    time.Duration
	SidecarTimeout   time.Duration // stats silence beyond this = suspect
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// HealthFunc probes a worker's model server.
type HealthFunc func(ctx context.Context, url string) error

// Worker is the router's live view of one worker.
type Worker struct {
	Endpoint
	Breaker *Breaker

	inflight atomic.Int64 // requests this router currently has on the worker

	mu       sync.Mutex
	healthy  bool                 // last active health check result
	stats    *relayv1.WorkerStats // last sidecar report
	statsAt  time.Time            // when it arrived
	epochKey string               // instance_id/epoch of the last report
	removed  bool                 // left membership; lingers until in-flight requests finish
	cancel   context.CancelFunc   // stops this worker's goroutines
}

// InFlight returns this router's in-flight request count on the worker.
func (w *Worker) InFlight() int { return int(w.inflight.Load()) }

// Begin/End bracket a request routed to this worker.
func (w *Worker) Begin() { w.inflight.Add(1) }
func (w *Worker) End()   { w.inflight.Add(-1) }

// View is a point-in-time copy of a worker's state.
type View struct {
	ID, URL     string
	Healthy     bool
	Breaker     string
	Draining    bool
	Suspect     bool // sidecar configured but silent
	InFlight    int
	Running     int
	Waiting     int
	Load        int // max(InFlight, Running+Waiting)
	KVUsage     float64
	KVCapacity  uint64
	PrefixHits  uint64
	PrefixQuery uint64
	StatsAgeMs  int64
	Epoch       uint64
	Removed     bool // left membership, finishing in-flight requests
	IndexBlocks int
	IndexCap    int
	HasSidecar  bool
	statsFresh  bool
}

// Pool is the set of known workers.
type Pool struct {
	cfg     Config
	health  HealthFunc
	indexes *prefix.Indexes
	log     *slog.Logger

	mu      sync.RWMutex
	workers map[string]*Worker
	ctx     context.Context
}

// New creates an empty pool. ctx bounds the lifetime of background work.
func New(ctx context.Context, cfg Config, health HealthFunc, indexes *prefix.Indexes, log *slog.Logger) *Pool {
	return &Pool{cfg: cfg, health: health, indexes: indexes, log: log, workers: map[string]*Worker{}, ctx: ctx}
}

// Indexes exposes the per-worker prefix indexes.
func (p *Pool) Indexes() *prefix.Indexes { return p.indexes }

// SetMembers reconciles the pool with a freshly discovered endpoint list:
// new workers start health checking and sidecar streaming, removed workers
// are stopped and their prefix index dropped.
func (p *Pool) SetMembers(eps []Endpoint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	for _, ep := range eps {
		seen[ep.ID] = true
		if w, ok := p.workers[ep.ID]; ok {
			if w.URL == ep.URL && w.SidecarAddr == ep.SidecarAddr {
				w.mu.Lock()
				w.removed = false // came back (e.g. readiness flapped)
				w.mu.Unlock()
				continue
			}
			w.cancel() // address changed: restart its goroutines
			p.indexes.Remove(ep.ID)
		}
		ctx, cancel := context.WithCancel(p.ctx)
		w := &Worker{
			Endpoint: ep,
			Breaker:  NewBreaker(p.cfg.BreakerThreshold, p.cfg.BreakerCooldown),
			healthy:  true, // optimistic until the first probe says otherwise
			cancel:   cancel,
		}
		p.workers[ep.ID] = w
		p.log.Info("worker added", "worker", ep.ID, "url", ep.URL, "sidecar", ep.SidecarAddr)
		go p.healthLoop(ctx, w)
		if ep.SidecarAddr != "" {
			go p.watchSidecar(ctx, w)
		}
	}
	for id, w := range p.workers {
		if seen[id] {
			continue
		}
		if w.InFlight() == 0 {
			p.purgeLocked(id, w)
			continue
		}
		// A draining pod leaves the Service (not ready) while it still
		// serves streams. Keep it, ineligible, until they finish; the health
		// loop purges it then.
		w.mu.Lock()
		if !w.removed {
			p.log.Info("worker left membership; lingering until in-flight requests finish", "worker", id, "inflight", w.InFlight())
		}
		w.removed = true
		w.mu.Unlock()
	}
}

func (p *Pool) purgeLocked(id string, w *Worker) {
	w.cancel()
	delete(p.workers, id)
	p.indexes.Remove(id)
	p.log.Info("worker removed", "worker", id)
}

// Get returns a worker by ID (nil if unknown).
func (p *Pool) Get(id string) *Worker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.workers[id]
}

// Size returns the number of known workers.
func (p *Pool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.workers)
}

// Eligible returns views of workers that may receive a NEW request:
// healthy, breaker allows, not draining, and not in exclude. Suspect workers
// (sidecar gone quiet) are used only if nothing else is available.
func (p *Pool) Eligible(exclude map[string]bool) []View {
	var ok, suspect []View
	for _, v := range p.Snapshot() {
		if exclude[v.ID] || !v.Healthy || v.Draining || v.Removed {
			continue
		}
		if w := p.Get(v.ID); w == nil || !w.Breaker.Allow() {
			continue
		}
		if v.Suspect {
			suspect = append(suspect, v)
		} else {
			ok = append(ok, v)
		}
	}
	if len(ok) == 0 {
		return suspect
	}
	return ok
}

// IsDown reports whether a worker should be treated as dead for requests
// already running on it: gone from the pool, failing health checks, its
// sidecar silent, or its breaker open. (Draining is NOT down: in-flight
// requests on a draining worker must be allowed to finish.)
func (p *Pool) IsDown(id string) bool {
	w := p.Get(id)
	if w == nil {
		return true
	}
	v := p.view(w)
	return !v.Healthy || v.Suspect || v.Breaker == Open.String()
}

// Healthy returns how many workers are currently eligible.
func (p *Pool) Healthy() int { return len(p.Eligible(nil)) }

// Snapshot returns a view of every worker, sorted by ID.
func (p *Pool) Snapshot() []View {
	p.mu.RLock()
	ws := make([]*Worker, 0, len(p.workers))
	for _, w := range p.workers {
		ws = append(ws, w)
	}
	p.mu.RUnlock()
	out := make([]View, 0, len(ws))
	for _, w := range ws {
		out = append(out, p.view(w))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (p *Pool) view(w *Worker) View {
	w.mu.Lock()
	defer w.mu.Unlock()
	ix := p.indexes.For(w.ID)
	v := View{
		ID: w.ID, URL: w.URL, Healthy: w.healthy, Breaker: w.Breaker.State().String(),
		InFlight: w.InFlight(), HasSidecar: w.SidecarAddr != "", Removed: w.removed,
		IndexBlocks: ix.Len(), IndexCap: ix.Capacity(),
	}
	if w.stats != nil {
		age := time.Since(w.statsAt)
		v.StatsAgeMs = age.Milliseconds()
		v.statsFresh = age <= p.cfg.SidecarTimeout
		v.Draining = w.stats.State == relayv1.WorkerState_WORKER_STATE_DRAINING
		v.Running, v.Waiting = int(w.stats.RunningRequests), int(w.stats.WaitingRequests)
		v.KVUsage, v.KVCapacity = w.stats.KvCacheUsage, w.stats.KvCacheCapacityTokens
		v.PrefixHits, v.PrefixQuery = w.stats.PrefixCacheHitTokens, w.stats.PrefixCacheQueryTokens
		v.Epoch = w.stats.Epoch
		if w.stats.State == relayv1.WorkerState_WORKER_STATE_UNAVAILABLE && v.statsFresh {
			v.Healthy = false // sidecar says the model server is down
		}
	}
	v.Suspect = v.HasSidecar && !v.statsFresh
	v.Load = v.InFlight
	if v.statsFresh && v.Running+v.Waiting > v.Load {
		v.Load = v.Running + v.Waiting
	}
	return v
}

// healthLoop actively probes the model server.
func (p *Pool) healthLoop(ctx context.Context, w *Worker) {
	t := time.NewTicker(p.cfg.HealthInterval)
	defer t.Stop()
	for {
		w.mu.Lock()
		gone := w.removed && w.InFlight() == 0
		w.mu.Unlock()
		if gone {
			p.mu.Lock()
			if cur := p.workers[w.ID]; cur == w {
				p.purgeLocked(w.ID, w)
			}
			p.mu.Unlock()
			return
		}
		hctx, cancel := context.WithTimeout(ctx, p.cfg.HealthTimeout)
		err := p.health(hctx, w.URL)
		cancel()
		if ctx.Err() != nil {
			return
		}
		w.mu.Lock()
		was := w.healthy
		w.healthy = err == nil
		w.mu.Unlock()
		if err != nil {
			w.Breaker.Failure()
			if was {
				p.log.Warn("worker unhealthy", "worker", w.ID, "err", err)
			}
		} else {
			if w.Breaker.State() == HalfOpen {
				w.Breaker.Success() // probe succeeded: close the breaker
			}
			if !was {
				p.log.Info("worker healthy again", "worker", w.ID)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// applyStats stores a sidecar report and reacts to restarts and capacity.
func (p *Pool) applyStats(w *Worker, s *relayv1.WorkerStats) {
	key := s.InstanceId + "/" + itoa(s.Epoch)
	w.mu.Lock()
	restarted := w.epochKey != "" && w.epochKey != key
	w.epochKey = key
	w.stats, w.statsAt = s, time.Now()
	w.mu.Unlock()

	ix := p.indexes.For(w.ID)
	if restarted {
		// The worker's KV cache is empty now; our beliefs about it are wrong.
		ix.Reset()
		p.log.Info("worker restart detected; prefix index cleared", "worker", w.ID, "epoch", s.Epoch)
	}
	if s.KvCacheCapacityTokens > 0 && p.cfg.BlockSize > 0 {
		if c := int(s.KvCacheCapacityTokens) / p.cfg.BlockSize; c != ix.Capacity() {
			ix.Resize(c)
		}
	}
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
