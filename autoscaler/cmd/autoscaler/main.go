// Command autoscaler scales the worker Deployment on queue depth and TTFT.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	as "github.com/yousufmohi/relay/autoscaler/internal"
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

func envDur(k string, d time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	targetQ, _ := strconv.ParseFloat(env("AUTOSCALER_TARGET_QUEUE_PER_WORKER", "4"), 64)
	t := as.Targets{
		QueuePerWorker: targetQ,
		TTFTP95:        time.Duration(envInt("AUTOSCALER_TARGET_TTFT_P95_MS", 800)) * time.Millisecond,
		Min:            envInt("AUTOSCALER_MIN_REPLICAS", 2),
		Max:            envInt("AUTOSCALER_MAX_REPLICAS", 8),
		UpWindow:       envDur("AUTOSCALER_UP_WINDOW", 20*time.Second),
		UpCooldown:     envDur("AUTOSCALER_UP_COOLDOWN", 30*time.Second),
		DownWindow:     envDur("AUTOSCALER_DOWN_WINDOW", 90*time.Second),
	}
	interval := envDur("AUTOSCALER_INTERVAL", 5*time.Second)
	httpc := &http.Client{Timeout: 3 * time.Second}

	var src as.Source
	if u := os.Getenv("PROMETHEUS_URL"); u != "" {
		src = &as.Prometheus{URL: u, Window: env("AUTOSCALER_WINDOW", "1m"), HTTP: httpc}
	} else {
		src = &as.RouterScrape{URLs: strings.Split(env("ROUTER_URLS", "http://localhost:8080"), ","),
			Window: envDur("AUTOSCALER_SCRAPE_WINDOW", 30*time.Second), HTTP: httpc}
	}
	var scaler as.Scaler
	if f := os.Getenv("SCALE_FILE"); f != "" {
		scaler = as.FileScaler{Path: f}
	} else {
		k, err := as.NewK8sScaler(env("NAMESPACE", "default"), env("WORKER_DEPLOYMENT", "relay-worker"), os.Getenv("KUBECONFIG"))
		if err != nil {
			log.Error("kubernetes client", "err", err)
			os.Exit(1)
		}
		scaler = k
	}

	reg := prometheus.NewRegistry()
	replicas := prometheus.NewGauge(prometheus.GaugeOpts{Name: "relay_autoscaler_replicas", Help: "Current worker replicas."})
	desired := prometheus.NewGauge(prometheus.GaugeOpts{Name: "relay_autoscaler_desired_replicas", Help: "Replicas the controller wants."})
	queue := prometheus.NewGauge(prometheus.GaugeOpts{Name: "relay_autoscaler_queue_per_worker", Help: "Observed queued requests per worker."})
	ttft := prometheus.NewGauge(prometheus.GaugeOpts{Name: "relay_autoscaler_ttft_p95_seconds", Help: "Observed TTFT p95."})
	decisions := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "relay_autoscaler_decisions_total", Help: "Scaling actions taken."}, []string{"direction", "reason"})
	reg.MustRegister(replicas, desired, queue, ttft, decisions)
	go http.ListenAndServe(env("METRICS_ADDR", ":9090"), promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	d := &as.Decider{T: t}
	log.Info("autoscaler started", "targets", t, "interval", interval)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		cur, err := scaler.Current(ctx)
		if err != nil {
			log.Warn("read replicas", "err", err)
		} else if q, p95, err := src.Read(ctx); err != nil {
			log.Warn("read metrics", "err", err)
		} else {
			dec := d.Decide(as.Observation{Now: time.Now(), Replicas: cur, QueueDepth: q, TTFTP95: p95})
			replicas.Set(float64(cur))
			desired.Set(float64(dec.Desired))
			queue.Set(q / float64(max(1, cur)))
			ttft.Set(p95.Seconds())
			if dec.Desired != cur {
				reason := strings.SplitN(dec.Reason, ":", 2)[0]
				decisions.WithLabelValues(dec.Direction, reason).Inc()
				log.Info("scaling", "from", cur, "to", dec.Desired, "direction", dec.Direction, "reason", dec.Reason)
				if err := scaler.Scale(ctx, dec.Desired); err != nil {
					log.Error("scale", "err", err)
				}
			} else {
				log.Debug("no change", "replicas", cur, "reason", dec.Reason)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
