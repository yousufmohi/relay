// Package server is the sidecar's core: a scrape loop that turns model
// server metrics into WorkerStats, a hub that fans them out to every
// connected router over gRPC, restart detection, and graceful draining.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	relayv1 "github.com/yousufmohi/relay/gen/relay/v1"
	"github.com/yousufmohi/relay/sidecar/internal/scrape"
)

// Sidecar implements relayv1.WorkerStatsServiceServer.
type Sidecar struct {
	relayv1.UnimplementedWorkerStatsServiceServer

	workerID   string
	instanceID string
	scraper    scrape.Scraper
	interval   time.Duration
	log        *slog.Logger

	mu       sync.Mutex
	last     *relayv1.WorkerStats
	subs     map[chan *relayv1.WorkerStats]struct{}
	seq      uint64
	epoch    uint64
	draining bool
	paused   bool // fault injection: stop streaming (looks like a partition)

	// restart detection state
	prevProgress uint64
	prevStart    float64
	wasDown      bool
	everUp       bool
}

func New(workerID string, sc scrape.Scraper, interval time.Duration, log *slog.Logger) *Sidecar {
	var b [8]byte
	rand.Read(b[:])
	return &Sidecar{workerID: workerID, instanceID: hex.EncodeToString(b[:]), scraper: sc, interval: interval,
		log: log, subs: map[chan *relayv1.WorkerStats]struct{}{}}
}

// Run scrapes every interval until ctx is done.
func (s *Sidecar) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		sctx, cancel := context.WithTimeout(ctx, max(s.interval*4, time.Second))
		sample, err := s.scraper.Scrape(sctx)
		cancel()
		s.publish(s.build(sample, err))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// build converts a scrape into WorkerStats, detecting model server restarts:
//   - cumulative counters went backwards, or
//   - process start time changed, or
//   - the server was unreachable and came back (its cache is likely gone).
//
// Each detected restart increments epoch; routers clear their prefix index
// for this worker when (instance_id, epoch) changes.
func (s *Sidecar) build(sm scrape.Sample, err error) *relayv1.WorkerStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	st := &relayv1.WorkerStats{WorkerId: s.workerID, InstanceId: s.instanceID, Seq: s.seq, ScrapedUnixMs: time.Now().UnixMilli()}
	if err != nil {
		if !s.wasDown {
			s.log.Warn("model server unreachable", "err", err)
		}
		s.wasDown = true
		st.State = relayv1.WorkerState_WORKER_STATE_UNAVAILABLE
		st.Epoch = s.epoch
		if s.draining {
			st.State = relayv1.WorkerState_WORKER_STATE_DRAINING
		}
		return st
	}
	restarted := s.everUp && (s.wasDown ||
		sm.Progress < s.prevProgress ||
		(sm.StartTime != 0 && s.prevStart != 0 && sm.StartTime != s.prevStart))
	if restarted {
		s.epoch++
		s.log.Info("model server restart detected", "epoch", s.epoch)
	}
	s.everUp, s.wasDown, s.prevProgress, s.prevStart = true, false, sm.Progress, sm.StartTime

	st.Epoch = s.epoch
	st.State = relayv1.WorkerState_WORKER_STATE_READY
	if s.draining {
		st.State = relayv1.WorkerState_WORKER_STATE_DRAINING
	}
	st.RunningRequests, st.WaitingRequests = uint32(sm.Running), uint32(sm.Waiting)
	st.KvCacheUsage, st.KvCacheCapacityTokens = sm.KVUsage, sm.CapacityTokens
	st.PrefixCacheHitTokens, st.PrefixCacheQueryTokens = sm.HitTokens, sm.QueryTokens
	st.Model = sm.Model
	return st
}

// publish stores the latest stats and sends them to every subscriber.
func (s *Sidecar) publish(st *relayv1.WorkerStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = st
	if s.paused {
		return
	}
	for ch := range s.subs {
		// Never block the scrape loop on a slow router: drop its stale
		// message and send the newest instead.
		select {
		case ch <- st:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- st:
			default:
			}
		}
	}
}

// Watch streams stats to one router until it disconnects.
func (s *Sidecar) Watch(stream relayv1.WorkerStatsService_WatchServer) error {
	ch := make(chan *relayv1.WorkerStats, 4)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	if s.last != nil && !s.paused {
		ch <- s.last // send the current state right away
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
	}()

	// Read router messages (hello / pings) so we notice disconnects.
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			s.log.Debug("router message", "router", msg.RouterId)
		}
	}()
	for {
		select {
		case st := <-ch:
			if err := stream.Send(&relayv1.WatchResponse{Stats: st}); err != nil {
				return err
			}
		case <-recvErr:
			return nil
		case <-stream.Context().Done():
			return nil
		}
	}
}

// StartDrain marks the worker draining and pushes that to routers at once.
func (s *Sidecar) StartDrain() {
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return
	}
	s.draining = true
	last := s.last
	s.mu.Unlock()
	s.log.Info("draining: routers will stop sending new requests")
	if last != nil {
		cp := proto.Clone(last).(*relayv1.WorkerStats)
		cp.State = relayv1.WorkerState_WORKER_STATE_DRAINING
		s.publish(cp)
	}
}

// Draining reports whether a drain was requested.
func (s *Sidecar) Draining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

// WaitDrained blocks until the model server reports no running or waiting
// requests on two consecutive scrapes, or ctx ends (grace period).
func (s *Sidecar) WaitDrained(ctx context.Context) bool {
	idle := 0
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		s.mu.Lock()
		last := s.last
		s.mu.Unlock()
		if last != nil && (last.State == relayv1.WorkerState_WORKER_STATE_UNAVAILABLE ||
			last.RunningRequests+last.WaitingRequests == 0) {
			idle++
		} else {
			idle = 0
		}
		if idle >= 2 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}

// SetPaused stops/resumes streaming (fault injection for local experiments).
func (s *Sidecar) SetPaused(p bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = p
}

// Last returns the most recent stats (for the HTTP debug endpoint).
func (s *Sidecar) Last() *relayv1.WorkerStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}
