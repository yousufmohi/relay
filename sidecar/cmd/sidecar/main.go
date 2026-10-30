// Command sidecar runs next to each model server. It scrapes the server's
// metrics, streams WorkerStats to every router over gRPC, and handles
// graceful draining on SIGTERM or POST /drain.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	relayv1 "github.com/yousufmohi/relay/gen/relay/v1"
	"github.com/yousufmohi/relay/sidecar/internal/scrape"
	"github.com/yousufmohi/relay/sidecar/internal/server"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	host, _ := os.Hostname()
	var (
		modelURL = env("MODEL_URL", "http://127.0.0.1:8080")
		flavor   = env("WORKER_FLAVOR", "llamacpp")
		grpcAddr = env("GRPC_ADDR", ":9500")
		httpAddr = env("HTTP_ADDR", ":9501")
		workerID = env("WORKER_ID", host)
		interval = time.Duration(envInt("SIDECAR_SCRAPE_MS", 250)) * time.Millisecond
		grace    = time.Duration(envInt("DRAIN_GRACE_S", 120)) * time.Second
	)

	sc := server.New(workerID, scrape.New(flavor, modelURL), interval, log.With("worker", workerID))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sc.Run(ctx)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	gs := grpc.NewServer()
	relayv1.RegisterWorkerStatsServiceServer(gs, sc)
	go gs.Serve(lis)

	// drain is shared by SIGTERM and POST /drain (used as the model
	// container's preStop hook, so the model server keeps running until its
	// in-flight streams have finished).
	drain := func() bool {
		sc.StartDrain()
		dctx, dcancel := context.WithTimeout(context.Background(), grace)
		defer dcancel()
		return sc.WaitDrained(dctx)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if sc.Draining() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready"))
	})
	mux.HandleFunc("/drain", func(w http.ResponseWriter, _ *http.Request) {
		if drain() {
			w.Write([]byte("drained"))
		} else {
			http.Error(w, "grace period elapsed with requests in flight", http.StatusGatewayTimeout)
		}
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sc.Last())
	})
	mux.HandleFunc("/admin/pause", func(w http.ResponseWriter, r *http.Request) {
		sc.SetPaused(r.URL.Query().Get("on") == "1")
		w.Write([]byte("ok"))
	})
	go http.ListenAndServe(httpAddr, mux)
	log.Info("sidecar running", "worker", workerID, "model", modelURL, "flavor", flavor, "grpc", grpcAddr, "http", httpAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Info("SIGTERM: draining")
	ok := drain()
	log.Info("drain finished", "clean", ok)
	gs.GracefulStop()
}
