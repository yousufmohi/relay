package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yousufmohi/relay/loadgen/internal/workload"
)

func sse(w http.ResponseWriter, tokens int) {
	w.Header().Set("Content-Type", "text/event-stream")
	for i := 0; i < tokens; i++ {
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"t%d \"}}]}\n\n", i)
		w.(http.Flusher).Flush()
	}
	fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"prompt_tokens_details\":{\"cached_tokens\":64}},\"relay\":{\"worker\":\"w1\",\"failovers\":1,\"failover_pause_ms\":[12.5],\"match_blocks\":1}}\n\n")
	fmt.Fprint(w, "data: [DONE]\n\n")
}

var msgs = []workload.Message{{Role: "user", Content: "hi"}}

func TestRecordsStreamMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sse(w, 3) }))
	defer srv.Close()
	var rec Record
	New([]string{srv.URL}).Do(context.Background(), time.Now(), msgs, &rec)
	if rec.Status != "ok" || rec.Tokens != 3 || len(rec.ITLMs) != 2 || rec.TTFTMs < 0 {
		t.Fatalf("%+v", rec)
	}
	if rec.PromptToks != 100 || rec.CachedToks != 64 || rec.Worker != "w1" || rec.Failovers != 1 || rec.PauseMs[0] != 12.5 {
		t.Fatalf("usage/relay ext not parsed: %+v", rec)
	}
}

func TestRespectsRetryAfter(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "busy", http.StatusTooManyRequests)
			return
		}
		sse(w, 1)
	}))
	defer srv.Close()
	var rec Record
	start := time.Now()
	New([]string{srv.URL}).Do(context.Background(), start, msgs, &rec)
	if rec.Status != "ok" || rec.Retries429 != 1 || time.Since(start) < time.Second {
		t.Fatalf("%+v after %v", rec, time.Since(start))
	}
}

func TestConnectionFailureRetriesOtherTarget(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sse(w, 2) }))
	defer good.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close() // connection refused
	c := New([]string{dead.URL, good.URL})
	c.next.Store(^uint64(0)) // first request goes to targets[0] (the dead one)
	var rec Record
	c.Do(context.Background(), time.Now(), msgs, &rec)
	if rec.Status != "ok" || rec.ConnRetries != 1 || rec.Target != good.URL {
		t.Fatalf("%+v", rec)
	}
}

func TestBrokenStreamIsRetried(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
			return // ends without [DONE], like a router dying mid-stream
		}
		sse(w, 2)
	}))
	defer srv.Close()
	var rec Record
	New([]string{srv.URL}).Do(context.Background(), time.Now(), msgs, &rec)
	if rec.Status != "ok" || rec.ConnRetries != 1 || rec.Tokens != 2 {
		t.Fatalf("%+v", rec)
	}
}
