package pool

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	relayv1 "github.com/yousufmohi/relay/gen/relay/v1"
)

// watchSidecar keeps one Watch stream open to the worker's sidecar for as
// long as the worker is in the pool, reconnecting with backoff.
func (p *Pool) watchSidecar(ctx context.Context, w *Worker) {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		start := time.Now()
		err := p.watchOnce(ctx, w)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > 5*time.Second {
			backoff = 200 * time.Millisecond // the stream was healthy for a while
		}
		p.log.Debug("sidecar stream ended", "worker", w.ID, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (p *Pool) watchOnce(ctx context.Context, w *Worker) error {
	conn, err := grpc.NewClient(w.SidecarAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second}),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := relayv1.NewWorkerStatsServiceClient(conn).Watch(sctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&relayv1.WatchRequest{RouterId: p.cfg.RouterID, SentUnixMs: time.Now().UnixMilli()}); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if s := msg.GetStats(); s != nil {
			p.applyStats(w, s)
		}
	}
}
