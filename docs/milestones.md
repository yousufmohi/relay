# Milestones

The project was built in eight milestones, each with acceptance criteria. Where a criterion was only partly met, [report.md](report.md) explains why.

### M0 — Skeleton
- [x] Repo layout, Makefile, protobuf contracts, CI (lint + unit tests)
- [x] docker compose with three llama.cpp CPU workers and a pass-through router
**Accept:** a streaming chat request through the router returns tokens from a worker.

### M1 — Load generator and baselines
- [x] `loadgen` with TTFT/inter-token measurement and all workloads
- [x] `round_robin`, `least_loaded`, `consistent_hash` policies
- [x] Results writer and plotting script
**Accept:** E1 runs on CPU for the three baselines and produces graphs.

### M2 — Sidecar and live worker stats
- [x] Sidecar scraping and gRPC streaming to the router; restart detection
**Accept:** the router's view of each worker's queue depth and cache usage matches the worker's own metrics within one scrape interval.

### M3 — Cache-aware routing
- [x] Block hashing, per-worker prefix index, `relay` scoring with bounded load
- [x] Unit tests: chained hashes, match lengths, LRU eviction, bounded-load exclusion, tie-breaking
**Accept:** on CPU, `relay` beats all baselines on cache hit rate and TTFT p99 for the shared-prefix workloads, doesn't lose on `unique`, and avoids the hotspot in E3.

### M4 — Resilience
- [x] Health checks, circuit breakers, admission control with 429s
- [x] Mid-stream failover with failover-pause metric
- [x] Two router replicas; sidecar draining on SIGTERM
**Accept:** E4 shows no client-visible errors when a worker is killed mid-stream; E6 shows no dropped requests beyond retried connections when a router is killed.

### M5 — Kubernetes and chaos
- [x] Helm chart, Prometheus, Grafana dashboard, OpenTelemetry traces
- [x] Chaos Mesh manifests for E4–E6
**Accept:** E4–E6 run on the k3s cluster via `make exp`, with graphs and dashboard screenshots.

### M6 — Autoscaler
- [x] Custom controller with up/down logic, cooldowns, bounds, decision metrics
**Accept:** E7 shows workers scaling up during a spike, TTFT recovering, and scale-down draining without dropped streams.

### M7 — GPU benchmarks and writeup
- [ ] Terraform GPU environment, vLLM workers, E1–E3 on GPU, E8 sweep *(Terraform, vLLM chart flavor, bench script and the E8 sweep (on the simulated cluster) are done; the GPU run itself has not been executed — it needs cloud credentials and spend)*
- [x] `docs/report.md`: problem, design, results with graphs, failure-mode analysis, what didn't work and why
**Accept:** the report contains the headline comparison table and every graph is reproducible from committed scripts.
