package routing

import (
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yousufmohi/relay/router/internal/prefix"
)

// ---------------------------------------------------------------- round_robin

// RoundRobin cycles through workers, ignoring load and cache state.
type RoundRobin struct{ next atomic.Uint64 }

func NewRoundRobin() *RoundRobin     { return &RoundRobin{} }
func (*RoundRobin) Name() string     { return "round_robin" }
func (*RoundRobin) NeedsMatch() bool { return false }

func (p *RoundRobin) Pick(_ Request, ws []Worker) (Decision, error) {
	if len(ws) == 0 {
		return Decision{}, ErrNoWorkers
	}
	// The eligible set can change between calls; sort by ID so the rotation is
	// stable for a given set regardless of the order the caller passed.
	sorted := sortedByID(ws)
	i := (p.next.Add(1) - 1) % uint64(len(sorted))
	return Decision{WorkerID: sorted[i].ID, Reason: "round_robin"}, nil
}

// --------------------------------------------------------------- least_loaded

// LeastLoaded picks the worker with the fewest outstanding requests.
type LeastLoaded struct{ rnd *lockedRand }

func NewLeastLoaded(seed int64) *LeastLoaded { return &LeastLoaded{rnd: newLockedRand(seed)} }
func (*LeastLoaded) Name() string            { return "least_loaded" }
func (*LeastLoaded) NeedsMatch() bool        { return false }

func (p *LeastLoaded) Pick(_ Request, ws []Worker) (Decision, error) {
	if len(ws) == 0 {
		return Decision{}, ErrNoWorkers
	}
	best := []Worker{}
	for _, w := range ws {
		switch {
		case len(best) == 0 || w.Load < best[0].Load:
			best = []Worker{w}
		case w.Load == best[0].Load:
			best = append(best, w)
		}
	}
	return Decision{WorkerID: p.rnd.pick(best).ID, Reason: "least_loaded"}, nil
}

// ------------------------------------------------------------ consistent_hash

// ConsistentHash maps the hash of the first N prompt blocks onto a hash ring.
// Requests sharing those blocks always go to the same worker (great cache
// affinity), with no load awareness (a hot prefix overloads one worker).
type ConsistentHash struct {
	hashBlocks, vnodes int

	mu      sync.Mutex
	ringKey string      // worker set the cached ring was built for
	points  []ringPoint // sorted by hash
}

type ringPoint struct {
	hash   uint64
	worker string
}

func NewConsistentHash(hashBlocks, vnodes int) *ConsistentHash {
	if hashBlocks < 1 {
		hashBlocks = 1
	}
	if vnodes < 1 {
		vnodes = 1
	}
	return &ConsistentHash{hashBlocks: hashBlocks, vnodes: vnodes}
}
func (*ConsistentHash) Name() string     { return "consistent_hash" }
func (*ConsistentHash) NeedsMatch() bool { return false }

// key is the chained hash of block N-1, which identifies the first N blocks.
// Short prompts (no full block) fall back to the whole-prompt hash.
func (p *ConsistentHash) key(req Request) uint64 {
	if len(req.Blocks) == 0 {
		return mix(uint64(req.Key))
	}
	n := min(p.hashBlocks, len(req.Blocks))
	return mix(uint64(req.Blocks[n-1]))
}

func (p *ConsistentHash) Pick(req Request, ws []Worker) (Decision, error) {
	if len(ws) == 0 {
		return Decision{}, ErrNoWorkers
	}
	points := p.ring(ws)
	k := p.key(req)
	// First ring point clockwise from k (wrapping around).
	i := sort.Search(len(points), func(i int) bool { return points[i].hash >= k })
	if i == len(points) {
		i = 0
	}
	return Decision{WorkerID: points[i].worker, Reason: "consistent_hash"}, nil
}

// ring returns the ring for exactly this worker set, rebuilding only when the
// set changes. Building the ring from the *eligible* set means a failed worker's
// keys move to its ring successor while everyone else's keys stay put.
func (p *ConsistentHash) ring(ws []Worker) []ringPoint {
	sorted := sortedByID(ws)
	key := ""
	for _, w := range sorted {
		key += w.ID + "\x00"
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if key == p.ringKey {
		return p.points
	}
	pts := make([]ringPoint, 0, len(sorted)*p.vnodes)
	for _, w := range sorted {
		for v := 0; v < p.vnodes; v++ {
			pts = append(pts, ringPoint{hash: mix(uint64(prefix.KeyOf(strTokens(w.ID + "#" + strconv.Itoa(v))))), worker: w.ID})
		}
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].hash < pts[j].hash })
	p.ringKey, p.points = key, pts
	return pts
}

// ------------------------------------------------------------------- helpers

func sortedByID(ws []Worker) []Worker {
	out := append([]Worker(nil), ws...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func strTokens(s string) []int32 {
	out := make([]int32, len(s))
	for i := 0; i < len(s); i++ {
		out[i] = int32(s[i])
	}
	return out
}

// mix is a 64-bit finalizer (splitmix64) that spreads FNV output evenly over
// the ring.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// lockedRand is a mutex-guarded rand for random tie-breaking.
type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newLockedRand(seed int64) *lockedRand {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &lockedRand{r: rand.New(rand.NewSource(seed))}
}

func (l *lockedRand) pick(ws []Worker) Worker {
	if len(ws) == 1 {
		return ws[0]
	}
	// Sort first so the result depends only on the seed, not input order.
	ws = sortedByID(ws)
	l.mu.Lock()
	defer l.mu.Unlock()
	return ws[l.r.Intn(len(ws))]
}
