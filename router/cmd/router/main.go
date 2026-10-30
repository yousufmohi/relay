// Command router is Relay's cache-aware LLM inference router.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yousufmohi/relay/router/internal/admission"
	"github.com/yousufmohi/relay/router/internal/api"
	"github.com/yousufmohi/relay/router/internal/chat"
	"github.com/yousufmohi/relay/router/internal/config"
	"github.com/yousufmohi/relay/router/internal/pool"
	"github.com/yousufmohi/relay/router/internal/prefix"
	"github.com/yousufmohi/relay/router/internal/routing"
	"github.com/yousufmohi/relay/router/internal/telemetry"
	"github.com/yousufmohi/relay/router/internal/tokenize"
	"github.com/yousufmohi/relay/router/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	if err := run(log); err != nil {
		log.Error("router exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.SetupTracing(ctx, "relay-router")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	rcfg := routing.DefaultConfig()
	rcfg.Alpha, rcfg.Beta, rcfg.Gamma, rcfg.LoadBoundC, rcfg.HashBlocks = cfg.Alpha, cfg.Beta, cfg.Gamma, cfg.LoadBoundC, cfg.HashBlocks
	rcfg.LoadBoundFloor = cfg.LoadFloor
	policy, err := routing.New(cfg.Policy, rcfg)
	if err != nil {
		return err
	}
	tmpl, err := chat.Get(cfg.ChatTemplate)
	if err != nil {
		return err
	}
	client := worker.NewClient(worker.Flavor(cfg.WorkerFlavor), cfg.WorkerModel)

	p := pool.New(ctx, pool.Config{
		RouterID: cfg.RouterID, BlockSize: cfg.BlockSize,
		HealthInterval: cfg.HealthInterval, HealthTimeout: time.Second,
		SidecarTimeout: cfg.SidecarTimeout, BreakerThreshold: cfg.BreakerThreshold, BreakerCooldown: cfg.BreakerCooldown,
	}, client.Health, prefix.NewIndexes(cfg.IndexCapacity), log)

	adm := admission.New(admission.Config{
		QueueMax: cfg.QueueMax,
		Capacity: func() int { return p.Healthy() * cfg.MaxInflightPerWorker },
	})

	// Membership.
	update := func(eps []pool.Endpoint) { p.SetMembers(eps); adm.Kick() }
	switch {
	case cfg.Workers != "":
		eps, err := pool.ParseEndpoints(cfg.Workers)
		if err != nil {
			return err
		}
		update(eps)
	case cfg.WorkersFile != "":
		go pool.WatchFile(ctx, cfg.WorkersFile, time.Second, update)
	default:
		go pool.WatchDNS(ctx, cfg.WorkerDNS, cfg.WorkerPort, cfg.SidecarPort, 2*time.Second, update)
	}
	// Workers recover / sidecars report capacity asynchronously: re-check
	// whether queued requests can be dispatched.
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				adm.Kick()
			}
		}
	}()

	var tok tokenize.Tokenizer = tokenize.Approx{}
	if cfg.Tokenizer == "remote" {
		var rr atomic.Uint32
		tok = tokenize.NewRemote(func(ctx context.Context, text string) ([]int32, error) {
			views := p.Eligible(nil)
			if len(views) == 0 {
				return nil, routing.ErrNoWorkers
			}
			tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			// Any worker will do (same model everywhere); spread the load.
			return client.Tokenize(tctx, views[int(rr.Add(1)%uint32(len(views)))].URL, text)
		}, 100_000)
	}

	metrics := telemetry.NewMetrics(p,
		func() float64 { return float64(adm.QueueDepth()) },
		func() float64 { return float64(adm.InFlight()) })

	srv := &api.Server{Cfg: cfg, Pool: p, Policy: policy, Admission: adm, Tokenizer: tok,
		Template: tmpl, Client: client, Metrics: metrics, Log: log}
	httpSrv := &http.Server{Addr: cfg.Listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("router listening", "addr", cfg.Listen, "policy", policy.Name(), "router_id", cfg.RouterID)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	// Graceful shutdown: stop accepting, let in-flight streams finish.
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
