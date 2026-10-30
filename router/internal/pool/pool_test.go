package pool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	relayv1 "github.com/yousufmohi/relay/gen/relay/v1"
	"github.com/yousufmohi/relay/router/internal/prefix"
)

func testPool(t *testing.T, health HealthFunc) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := Config{RouterID: "r", BlockSize: 4, HealthInterval: 10 * time.Millisecond, HealthTimeout: time.Second,
		SidecarTimeout: time.Second, BreakerThreshold: 2, BreakerCooldown: time.Hour}
	if health == nil {
		health = func(context.Context, string) error { return nil }
	}
	return New(ctx, cfg, health, prefix.NewIndexes(100), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func ids(vs []View) string {
	s := ""
	for _, v := range vs {
		s += v.ID
	}
	return s
}

func TestParseEndpoints(t *testing.T) {
	eps, err := ParseEndpoints("a=http://h1:8080|h1:9500, b=http://h2:8080\n# comment\n")
	if err != nil || len(eps) != 2 {
		t.Fatal(eps, err)
	}
	if eps[0] != (Endpoint{"a", "http://h1:8080", "h1:9500"}) || eps[1].SidecarAddr != "" {
		t.Fatalf("%+v", eps)
	}
	if _, err := ParseEndpoints("nonsense"); err == nil {
		t.Fatal("want error")
	}
}

func TestMembershipReconcile(t *testing.T) {
	p := testPool(t, nil)
	p.SetMembers([]Endpoint{{ID: "a", URL: "u1"}, {ID: "b", URL: "u2"}})
	p.Indexes().For("b").Insert([]prefix.Hash{1, 2})
	p.SetMembers([]Endpoint{{ID: "a", URL: "u1"}, {ID: "c", URL: "u3"}})
	if got := ids(p.Snapshot()); got != "ac" {
		t.Fatalf("members %s", got)
	}
	if p.Indexes().For("b").Len() != 0 {
		t.Fatal("removed worker's index should be dropped")
	}
}

func TestEligibilityFilters(t *testing.T) {
	var fail atomic.Bool
	p := testPool(t, func(_ context.Context, url string) error {
		if url == "bad" && fail.Load() {
			return errors.New("down")
		}
		return nil
	})
	p.SetMembers([]Endpoint{{ID: "a", URL: "ok"}, {ID: "b", URL: "bad"}, {ID: "c", URL: "ok"}})
	if got := ids(p.Eligible(nil)); got != "abc" {
		t.Fatalf("all eligible initially: %s", got)
	}
	if got := ids(p.Eligible(map[string]bool{"a": true})); got != "bc" {
		t.Fatalf("exclude: %s", got)
	}
	fail.Store(true)
	time.Sleep(50 * time.Millisecond)
	if got := ids(p.Eligible(nil)); got != "ac" {
		t.Fatalf("unhealthy b must be excluded: %s", got)
	}
	p.Get("c").Breaker.Trip()
	if got := ids(p.Eligible(nil)); got != "a" {
		t.Fatalf("open breaker excludes c: %s", got)
	}
}

func TestDrainingAndLoadFromStats(t *testing.T) {
	p := testPool(t, nil)
	p.SetMembers([]Endpoint{{ID: "a", URL: "u"}, {ID: "b", URL: "u"}})
	p.applyStats(p.Get("a"), &relayv1.WorkerStats{InstanceId: "i", State: relayv1.WorkerState_WORKER_STATE_DRAINING})
	p.applyStats(p.Get("b"), &relayv1.WorkerStats{InstanceId: "i", State: relayv1.WorkerState_WORKER_STATE_READY, RunningRequests: 3, WaitingRequests: 2, KvCacheUsage: 0.5})
	if got := ids(p.Eligible(nil)); got != "b" {
		t.Fatalf("draining worker excluded: %s", got)
	}
	v := p.Eligible(nil)[0]
	if v.Load != 5 || v.KVUsage != 0.5 {
		t.Fatalf("load from stats: %+v", v)
	}
	// Our own in-flight count wins when it is higher than the sidecar's view.
	for i := 0; i < 7; i++ {
		p.Get("b").Begin()
	}
	if v := p.Eligible(nil)[0]; v.Load != 7 {
		t.Fatalf("load should be max(inflight, running+waiting): %d", v.Load)
	}
}

func TestRestartClearsIndexAndCapacityResizes(t *testing.T) {
	p := testPool(t, nil)
	p.SetMembers([]Endpoint{{ID: "a", URL: "u"}})
	w := p.Get("a")
	p.applyStats(w, &relayv1.WorkerStats{InstanceId: "x", Epoch: 0, KvCacheCapacityTokens: 40})
	if c := p.Indexes().For("a").Capacity(); c != 10 {
		t.Fatalf("capacity = tokens/blocksize = 10, got %d", c)
	}
	p.Indexes().For("a").Insert([]prefix.Hash{1, 2, 3})
	p.applyStats(w, &relayv1.WorkerStats{InstanceId: "x", Epoch: 0, KvCacheCapacityTokens: 40})
	if p.Indexes().For("a").Len() != 3 {
		t.Fatal("same epoch keeps index")
	}
	p.applyStats(w, &relayv1.WorkerStats{InstanceId: "x", Epoch: 1, KvCacheCapacityTokens: 40})
	if p.Indexes().For("a").Len() != 0 {
		t.Fatal("epoch change must clear index")
	}
	p.Indexes().For("a").Insert([]prefix.Hash{1})
	p.applyStats(w, &relayv1.WorkerStats{InstanceId: "y", Epoch: 1, KvCacheCapacityTokens: 40})
	if p.Indexes().For("a").Len() != 0 {
		t.Fatal("new sidecar instance must clear index")
	}
}

func TestSuspectOnlyAsLastResort(t *testing.T) {
	p := testPool(t, nil)
	// Sidecar address that never answers: worker a never gets stats → suspect.
	p.SetMembers([]Endpoint{{ID: "a", URL: "u", SidecarAddr: "127.0.0.1:1"}, {ID: "b", URL: "u"}})
	if got := ids(p.Eligible(nil)); got != "b" {
		t.Fatalf("suspect a avoided: %s", got)
	}
	if got := ids(p.Eligible(map[string]bool{"b": true})); got != "a" {
		t.Fatalf("suspect used when nothing else: %s", got)
	}
	p.applyStats(p.Get("a"), &relayv1.WorkerStats{InstanceId: "i", State: relayv1.WorkerState_WORKER_STATE_READY})
	if got := ids(p.Eligible(nil)); got != "ab" {
		t.Fatalf("fresh stats clear suspicion: %s", got)
	}
}

func TestRemovedWorkerLingersWhileServing(t *testing.T) {
	p := testPool(t, nil)
	p.SetMembers([]Endpoint{{ID: "a", URL: "u"}, {ID: "b", URL: "u"}})
	p.Get("a").Begin() // a request is streaming on a
	p.SetMembers([]Endpoint{{ID: "b", URL: "u"}})
	if p.Get("a") == nil {
		t.Fatal("worker with in-flight requests must linger")
	}
	if got := ids(p.Eligible(nil)); got != "b" {
		t.Fatalf("lingering worker must not get new requests: %s", got)
	}
	if p.IsDown("a") {
		t.Fatal("its running streams must not be aborted")
	}
	p.Get("a").End()
	time.Sleep(50 * time.Millisecond) // health loop purges it
	if p.Get("a") != nil {
		t.Fatal("purged after its last request")
	}
}
