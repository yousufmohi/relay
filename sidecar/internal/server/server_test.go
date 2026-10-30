package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	relayv1 "github.com/yousufmohi/relay/gen/relay/v1"
	"github.com/yousufmohi/relay/sidecar/internal/scrape"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRestartDetection(t *testing.T) {
	s := New("w", nil, time.Millisecond, quiet)
	st := s.build(scrape.Sample{Progress: 100}, nil)
	if st.Epoch != 0 || st.State != relayv1.WorkerState_WORKER_STATE_READY {
		t.Fatalf("%v", st)
	}
	if s.build(scrape.Sample{Progress: 150}, nil).Epoch != 0 {
		t.Fatal("growing counters: same epoch")
	}
	if s.build(scrape.Sample{Progress: 10}, nil).Epoch != 1 {
		t.Fatal("counters went backwards: restart")
	}
	if st := s.build(scrape.Sample{}, errors.New("down")); st.State != relayv1.WorkerState_WORKER_STATE_UNAVAILABLE {
		t.Fatal("scrape error → unavailable")
	}
	if s.build(scrape.Sample{Progress: 500}, nil).Epoch != 2 {
		t.Fatal("came back after being down: restart")
	}
	if s.build(scrape.Sample{Progress: 600, StartTime: 1}, nil).Epoch != 2 {
		t.Fatal("first start time seen: no restart")
	}
	if s.build(scrape.Sample{Progress: 700, StartTime: 2}, nil).Epoch != 3 {
		t.Fatal("start time changed: restart")
	}
}

type fakeScraper struct {
	mu sync.Mutex
	s  scrape.Sample
}

func (f *fakeScraper) Scrape(context.Context) (scrape.Sample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s, nil
}

func (f *fakeScraper) set(s scrape.Sample) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.s = s
}

func TestStreamAndDrain(t *testing.T) {
	fs := &fakeScraper{s: scrape.Sample{Running: 2, Waiting: 1, KVUsage: 0.3, CapacityTokens: 4096}}
	s := New("w1", fs, 5*time.Millisecond, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	relayv1.RegisterWorkerStatsServiceServer(gs, s)
	go gs.Serve(lis)
	defer gs.Stop()
	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := relayv1.NewWorkerStatsServiceClient(conn).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&relayv1.WatchRequest{RouterId: "r1"})
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	st := msg.Stats
	if st.WorkerId != "w1" || st.RunningRequests != 2 || st.WaitingRequests != 1 || st.KvCacheCapacityTokens != 4096 {
		t.Fatalf("%v", st)
	}

	s.StartDrain()
	deadline := time.After(time.Second)
	for {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Stats.State == relayv1.WorkerState_WORKER_STATE_DRAINING {
			break
		}
		select {
		case <-deadline:
			t.Fatal("draining state not streamed")
		default:
		}
	}
	// In-flight work keeps the drain waiting; it completes once idle.
	dctx, dcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if s.WaitDrained(dctx) {
		t.Fatal("must not report drained while requests run")
	}
	dcancel()
	fs.set(scrape.Sample{})
	dctx, dcancel = context.WithTimeout(context.Background(), time.Second)
	defer dcancel()
	if !s.WaitDrained(dctx) {
		t.Fatal("should drain once idle")
	}
}
