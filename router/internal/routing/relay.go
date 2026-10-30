package routing

import (
	"fmt"
	"math"
)

// Relay is the cache-aware policy:
//
//	score(w) = α·match_blocks(w) − β·queue_depth(w) − γ·kv_cache_usage(w)
//
// restricted to workers that pass the bounded-load test. The highest score
// wins; ties go to the worker with fewer in-flight requests, then random.
//
// Why each term:
//   - α·match_blocks rewards workers that can skip recomputing the prompt
//     prefix (a cache hit saves prefill time, which is most of TTFT).
//   - β·queue_depth penalises busy workers: a cache hit is worthless if the
//     request waits behind a long queue.
//   - γ·kv_cache_usage nudges away from workers whose KV cache is nearly
//     full (they will evict, or preempt, soon).
//
// Bounded load (from "Consistent Hashing with Bounded Loads", Mirrokni et
// al.): with n workers and total load L, a worker may take the request only
// if its load after taking it stays within c × the average:
//
//	load(w) + 1 <= ceil(c × (L + 1) / n)
//
// This is what stops a hot prefix (one popular system prompt) from piling all
// traffic onto the one worker that has it cached: once that worker is c×
// over average, the request spills to the best-scoring worker that is not.
//
// Optional floor: with few workers and light load the integer bound is very
// strict (4 workers, one busy with 1 request, others idle → limit 1, so the
// busy one is excluded although it has free slots). LoadBoundFloor (e.g. the
// worker's slot count) exempts workers whose load would stay at or below it.
// 0 disables the floor (the plain rule above).
type Relay struct {
	cfg Config
	rnd *lockedRand
}

func NewRelay(cfg Config) *Relay {
	if cfg.LoadBoundC < 1 {
		cfg.LoadBoundC = 1 // c < 1 would make every worker ineligible
	}
	return &Relay{cfg: cfg, rnd: newLockedRand(cfg.Seed)}
}

func (*Relay) Name() string     { return "relay" }
func (*Relay) NeedsMatch() bool { return true }

// Score computes the relay score for one worker. Exported for tests/tracing.
func (p *Relay) Score(w Worker) float64 {
	return p.cfg.Alpha*float64(w.MatchBlocks) - p.cfg.Beta*float64(w.Load) - p.cfg.Gamma*w.KVUsage
}

// LoadLimit returns the maximum load a worker may have AFTER accepting the
// request, given the current total load across n workers.
func LoadLimit(c float64, totalLoad, n int) int {
	return int(math.Ceil(c * float64(totalLoad+1) / float64(n)))
}

func (p *Relay) Pick(_ Request, ws []Worker) (Decision, error) {
	if len(ws) == 0 {
		return Decision{}, ErrNoWorkers
	}

	// 1. Bounded load: drop workers that would exceed c × average load.
	total := 0
	for _, w := range ws {
		total += w.Load
	}
	limit := max(LoadLimit(p.cfg.LoadBoundC, total, len(ws)), p.cfg.LoadBoundFloor)
	d := Decision{Scores: make(map[string]float64, len(ws))}
	var eligible []Worker
	for _, w := range ws {
		if w.Load+1 > limit {
			d.Excluded = append(d.Excluded, w.ID)
			continue
		}
		eligible = append(eligible, w)
	}
	// The least-loaded worker always satisfies the bound (its load is at most
	// the average), so eligible is never empty. Guard anyway.
	if len(eligible) == 0 {
		eligible = ws
	}

	// 2. Score; keep every worker tied for the best score.
	var best []Worker
	bestScore := math.Inf(-1)
	for _, w := range eligible {
		s := p.Score(w)
		d.Scores[w.ID] = s
		switch {
		case s > bestScore:
			best, bestScore = []Worker{w}, s
		case s == bestScore:
			best = append(best, w)
		}
	}

	// 3. Tie-break: fewer in-flight requests from this router, then random.
	if len(best) > 1 {
		minIF := best[0].InFlight
		for _, w := range best[1:] {
			minIF = min(minIF, w.InFlight)
		}
		var fewest []Worker
		for _, w := range best {
			if w.InFlight == minIF {
				fewest = append(fewest, w)
			}
		}
		best = fewest
	}
	chosen := p.rnd.pick(best)

	d.WorkerID = chosen.ID
	d.MatchBlocks = chosen.MatchBlocks
	d.Reason = fmt.Sprintf("score=%.2f match=%d load=%d limit=%d", bestScore, chosen.MatchBlocks, chosen.Load, limit)
	return d, nil
}
