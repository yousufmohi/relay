package routing

import (
	"testing"

	"github.com/yousufmohi/relay/router/internal/prefix"
)

func ws(ids ...string) []Worker {
	out := make([]Worker, len(ids))
	for i, id := range ids {
		out[i] = Worker{ID: id}
	}
	return out
}

func blocks(n int, start int32) []prefix.Hash {
	toks := make([]int32, n*4)
	for i := range toks {
		toks[i] = start + int32(i)
	}
	return prefix.BlockHashes(toks, 4)
}

func TestNewUnknownPolicy(t *testing.T) {
	if _, err := New("nope", DefaultConfig()); err == nil {
		t.Fatal("want error")
	}
	for _, n := range []string{"round_robin", "least_loaded", "consistent_hash", "relay"} {
		p, err := New(n, DefaultConfig())
		if err != nil || p.Name() != n {
			t.Fatalf("%s: %v", n, err)
		}
	}
}

func TestAllPoliciesNoWorkers(t *testing.T) {
	for _, n := range []string{"round_robin", "least_loaded", "consistent_hash", "relay"} {
		p, _ := New(n, DefaultConfig())
		if _, err := p.Pick(Request{}, nil); err != ErrNoWorkers {
			t.Fatalf("%s: want ErrNoWorkers, got %v", n, err)
		}
	}
}

func TestRoundRobinCycles(t *testing.T) {
	p := NewRoundRobin()
	got := map[string]int{}
	for i := 0; i < 9; i++ {
		d, _ := p.Pick(Request{}, ws("c", "a", "b"))
		got[d.WorkerID]++
	}
	if got["a"] != 3 || got["b"] != 3 || got["c"] != 3 {
		t.Fatalf("uneven rotation: %v", got)
	}
}

func TestLeastLoaded(t *testing.T) {
	p := NewLeastLoaded(1)
	w := []Worker{{ID: "a", Load: 3}, {ID: "b", Load: 1}, {ID: "c", Load: 2}}
	if d, _ := p.Pick(Request{}, w); d.WorkerID != "b" {
		t.Fatalf("want b, got %s", d.WorkerID)
	}
}

func TestLeastLoadedTiesAreRandomButFair(t *testing.T) {
	p := NewLeastLoaded(42)
	w := []Worker{{ID: "a", Load: 1}, {ID: "b", Load: 1}, {ID: "c", Load: 5}}
	got := map[string]int{}
	for i := 0; i < 1000; i++ {
		d, _ := p.Pick(Request{}, w)
		got[d.WorkerID]++
	}
	if got["c"] != 0 || got["a"] < 400 || got["b"] < 400 {
		t.Fatalf("bad tie distribution: %v", got)
	}
}

func TestConsistentHashSticky(t *testing.T) {
	p := NewConsistentHash(1, 64)
	req := Request{Blocks: blocks(3, 0)}
	first, _ := p.Pick(req, ws("a", "b", "c", "d"))
	for i := 0; i < 20; i++ {
		d, _ := p.Pick(req, ws("d", "c", "b", "a")) // order must not matter
		if d.WorkerID != first.WorkerID {
			t.Fatal("same prefix must map to same worker")
		}
	}
}

func TestConsistentHashUsesOnlyFirstNBlocks(t *testing.T) {
	p := NewConsistentHash(2, 64)
	a := blocks(5, 0)
	b := append(append([]prefix.Hash{}, a[:2]...), blocks(3, 9000)...) // same first 2 blocks
	da, _ := p.Pick(Request{Blocks: a}, ws("a", "b", "c", "d", "e"))
	db, _ := p.Pick(Request{Blocks: b}, ws("a", "b", "c", "d", "e"))
	if da.WorkerID != db.WorkerID {
		t.Fatal("requests sharing the first N blocks must route together")
	}
}

func TestConsistentHashMinimalMovement(t *testing.T) {
	p := NewConsistentHash(1, 128)
	all := ws("a", "b", "c", "d")
	without := ws("a", "b", "c")
	moved, total := 0, 400
	for i := 0; i < total; i++ {
		req := Request{Blocks: blocks(1, int32(i*10))}
		before, _ := p.Pick(req, all)
		after, _ := p.Pick(req, without)
		if before.WorkerID != "d" && before.WorkerID != after.WorkerID {
			moved++
		}
	}
	if moved != 0 {
		t.Fatalf("keys not on the removed worker must not move; %d moved", moved)
	}
}

func TestConsistentHashSpreadsKeys(t *testing.T) {
	p := NewConsistentHash(1, 128)
	got := map[string]int{}
	for i := 0; i < 2000; i++ {
		d, _ := p.Pick(Request{Blocks: blocks(1, int32(i*10))}, ws("a", "b", "c", "d"))
		got[d.WorkerID]++
	}
	for id, n := range got {
		if n < 300 || n > 700 {
			t.Fatalf("worker %s got %d of 2000 keys: %v", id, n, got)
		}
	}
}

func TestConsistentHashShortPromptUsesKey(t *testing.T) {
	p := NewConsistentHash(1, 64)
	d1, _ := p.Pick(Request{Key: 12345}, ws("a", "b", "c"))
	d2, _ := p.Pick(Request{Key: 12345}, ws("a", "b", "c"))
	if d1.WorkerID != d2.WorkerID {
		t.Fatal("same key, same worker")
	}
}

func relayCfg() Config {
	c := DefaultConfig()
	c.Seed = 7
	return c
}

func TestRelayPrefersCacheMatch(t *testing.T) {
	p := NewRelay(relayCfg())
	w := []Worker{{ID: "a", MatchBlocks: 0, Load: 1}, {ID: "b", MatchBlocks: 10, Load: 2}, {ID: "c", Load: 1}}
	d, _ := p.Pick(Request{}, w)
	if d.WorkerID != "b" {
		t.Fatalf("want b (cache match), got %s (%v)", d.WorkerID, d.Scores)
	}
	if d.MatchBlocks != 10 {
		t.Fatalf("decision should report match length, got %d", d.MatchBlocks)
	}
}

func TestRelayScoreFormula(t *testing.T) {
	p := NewRelay(Config{Alpha: 2, Beta: 0.5, Gamma: 4, LoadBoundC: 1.25})
	got := p.Score(Worker{MatchBlocks: 3, Load: 2, KVUsage: 0.25})
	if want := 2*3 - 0.5*2 - 4*0.25; got != want {
		t.Fatalf("score: want %v got %v", want, got)
	}
}

func TestRelayWithoutMatchesActsLeastLoaded(t *testing.T) {
	// The unique workload: no prefix matches anywhere, so relay must fall back
	// to load balancing and not lose to least_loaded.
	p := NewRelay(relayCfg())
	w := []Worker{{ID: "a", Load: 3}, {ID: "b", Load: 0}, {ID: "c", Load: 2}}
	if d, _ := p.Pick(Request{}, w); d.WorkerID != "b" {
		t.Fatalf("want b, got %s", d.WorkerID)
	}
}

func TestRelayKVUsageBreaksOtherwiseEqual(t *testing.T) {
	p := NewRelay(relayCfg())
	w := []Worker{{ID: "a", KVUsage: 0.9}, {ID: "b", KVUsage: 0.1}}
	if d, _ := p.Pick(Request{}, w); d.WorkerID != "b" {
		t.Fatalf("want b (less KV pressure), got %s", d.WorkerID)
	}
}

func TestLoadLimit(t *testing.T) {
	cases := []struct {
		c        float64
		total, n int
		want     int
	}{
		{1.25, 0, 4, 1},  // idle cluster: everyone may take 1
		{1.25, 8, 4, 3},  // avg after = 9/4 = 2.25 → ×1.25 = 2.81 → 3
		{1.0, 3, 3, 2},   // c=1: strict average, ceil(4/3)=2
		{2.0, 10, 2, 11}, // ceil(2×11/2)
	}
	for _, tc := range cases {
		if got := LoadLimit(tc.c, tc.total, tc.n); got != tc.want {
			t.Errorf("LoadLimit(%v,%d,%d)=%d want %d", tc.c, tc.total, tc.n, got, tc.want)
		}
	}
}

func TestRelayBoundedLoadExcludesHotWorker(t *testing.T) {
	// Worker a has the whole prompt cached but is already far above average
	// load: bounded load must make it ineligible regardless of the match.
	p := NewRelay(relayCfg())
	w := []Worker{
		{ID: "a", MatchBlocks: 100, Load: 6},
		{ID: "b", MatchBlocks: 0, Load: 1},
		{ID: "c", MatchBlocks: 0, Load: 1},
	}
	// total=8, n=3 → limit = ceil(1.25×9/3) = 4; a would be at 7 > 4.
	d, _ := p.Pick(Request{}, w)
	if d.WorkerID == "a" {
		t.Fatal("hot worker must be excluded by bounded load")
	}
	if len(d.Excluded) != 1 || d.Excluded[0] != "a" {
		t.Fatalf("excluded = %v", d.Excluded)
	}
}

func TestRelayBoundedLoadAllowsModestImbalance(t *testing.T) {
	p := NewRelay(relayCfg())
	w := []Worker{
		{ID: "a", MatchBlocks: 20, Load: 3},
		{ID: "b", Load: 2},
		{ID: "c", Load: 2},
	}
	// total=7 → limit = ceil(1.25×8/3)=4; a goes to 4, allowed.
	if d, _ := p.Pick(Request{}, w); d.WorkerID != "a" {
		t.Fatalf("within bound, cache match should win; got %s", d.WorkerID)
	}
}

func TestRelayHotPrefixSpreadsUnderLoad(t *testing.T) {
	// Simulate a hot prefix: every request matches only on worker "a" until
	// another worker serves it (then both have it cached). Loads accumulate
	// (no completions). Bounded load must keep the max load within c × avg.
	p := NewRelay(relayCfg())
	loads := map[string]int{"a": 0, "b": 0, "c": 0, "d": 0}
	cached := map[string]bool{"a": true}
	for i := 0; i < 40; i++ {
		var w []Worker
		for _, id := range []string{"a", "b", "c", "d"} {
			m := 0
			if cached[id] {
				m = 30
			}
			w = append(w, Worker{ID: id, Load: loads[id], MatchBlocks: m})
		}
		d, _ := p.Pick(Request{}, w)
		loads[d.WorkerID]++
		cached[d.WorkerID] = true
	}
	for id, l := range loads {
		if l > 13 { // ceil(1.25 × 40/4) = 13
			t.Fatalf("worker %s load %d exceeds bound: %v", id, l, loads)
		}
	}
}

func TestRelayTieBreakFewerInFlight(t *testing.T) {
	p := NewRelay(relayCfg())
	// Same score (same match, load, kv); b has fewer of OUR in-flight requests.
	w := []Worker{{ID: "a", Load: 2, InFlight: 2}, {ID: "b", Load: 2, InFlight: 0}}
	for i := 0; i < 20; i++ {
		if d, _ := p.Pick(Request{}, w); d.WorkerID != "b" {
			t.Fatalf("want b, got %s", d.WorkerID)
		}
	}
}

func TestRelayTieBreakRandom(t *testing.T) {
	p := NewRelay(relayCfg())
	w := []Worker{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	got := map[string]int{}
	for i := 0; i < 900; i++ {
		d, _ := p.Pick(Request{}, w)
		got[d.WorkerID]++
	}
	for _, id := range []string{"a", "b", "c"} {
		if got[id] < 200 {
			t.Fatalf("random tie-break should spread: %v", got)
		}
	}
}

func TestRelayClampsCBelowOne(t *testing.T) {
	p := NewRelay(Config{Alpha: 1, LoadBoundC: 0.1})
	if _, err := p.Pick(Request{}, ws("a", "b")); err != nil {
		t.Fatal(err)
	}
}

func TestRelayLoadBoundFloor(t *testing.T) {
	w := []Worker{{ID: "a", MatchBlocks: 20, Load: 1}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	// Plain rule: total=1 → limit = ceil(1.25×2/4) = 1, a would reach 2 → excluded.
	if d, _ := NewRelay(relayCfg()).Pick(Request{}, w); d.WorkerID == "a" {
		t.Fatal("without a floor the busy worker is excluded")
	}
	cfg := relayCfg()
	cfg.LoadBoundFloor = 4
	if d, _ := NewRelay(cfg).Pick(Request{}, w); d.WorkerID != "a" {
		t.Fatalf("with floor 4 the cache match wins; got %s", d.WorkerID)
	}
	// Above the floor the normal bound applies again.
	w[0].Load = 6
	if d, _ := NewRelay(cfg).Pick(Request{}, w); d.WorkerID == "a" {
		t.Fatal("above the floor the bound must still exclude")
	}
}
