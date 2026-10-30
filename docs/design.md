# Relay design

How the router, sidecar and autoscaler work, and what each experiment measures. Results are in [report.md](report.md); design decisions made along the way are in [decisions.md](decisions.md).

## Architecture

```
  clients / load generator
            │  OpenAI-compatible streaming API (SSE)
            ▼
   ┌──────────────────┐   ┌──────────────────┐
   │ router replica A │   │ router replica B │   (stateless-ish: soft state only)
   └───────┬──────────┘   └──────────┬───────┘
           │   gRPC stream: load + cache stats   │
           ▼                                     ▼
   ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
   │ worker pod 1  │  │ worker pod 2  │  │ worker pod N  │
   │ ┌───────────┐ │  │               │  │               │
   │ │ model     │ │  │      ...      │  │      ...      │
   │ │ server    │ │  │               │  │               │
   │ ├───────────┤ │  │               │  │               │
   │ │ sidecar   │ │  │               │  │               │
   │ └───────────┘ │  │               │  │               │
   └───────────────┘  └───────────────┘  └───────────────┘
            ▲
   autoscaler (Go controller) ── scales worker Deployment on queue depth / TTFT
```

### Components

| Component | Language | Role |
|---|---|---|
| `router` | Go | Public OpenAI-compatible API, routing, admission control, stream proxying, failover |
| `sidecar` | Go | Runs next to each model server; scrapes its metrics and streams load + cache stats to all routers over gRPC; handles graceful drain |
| model server | — | `llama.cpp` server (CPU, development) or `vLLM` (GPU, benchmarks), both with prefix caching enabled |
| `autoscaler` | Go (client-go) | Custom Kubernetes controller that scales workers on queue depth and TTFT |
| `loadgen` | Go | Workload generator that measures TTFT, inter-token latency and errors per stream |
| `experiments` | Python | Runs experiments, collects results, plots graphs |
| `simworker` | Go | llama.cpp-compatible simulated worker with a prefix-cache cost model, for laptop-scale experiments |

---

## Tech stack

| Area | Choice |
|---|---|
| Router, sidecar, autoscaler, load generator | Go |
| Router ↔ sidecar | gRPC (bidirectional streaming), protobuf in `proto/` |
| Client API | OpenAI-compatible `/v1/chat/completions` and `/v1/completions` with SSE streaming |
| Model servers | llama.cpp server with a small instruct model (e.g. Qwen2.5-0.5B/1.5B-Instruct GGUF) for CPU development; vLLM with automatic prefix caching and a 7–8B instruct model for GPU benchmarks |
| Cluster | k3s; Helm chart in `deploy/helm/relay/` |
| Fault injection | Chaos Mesh (pod kill, network delay, network partition) |
| Observability | Prometheus, Grafana (dashboards committed as JSON), OpenTelemetry tracing |
| GPU benchmark environment | Terraform: one or two GPU cloud VMs (spot/preemptible) running k3s, created for a benchmark session and destroyed afterwards |
| Experiment analysis | Python, pandas, matplotlib |
| CI | GitHub Actions: lint, unit tests, a short CPU-only integration test on a kind/k3d cluster |

---

## Repository layout

```
relay/
├── README.md
├── Makefile, Dockerfile     # one multi-stage Dockerfile, --build-arg COMPONENT=...
├── docs/
│   ├── decisions.md
│   ├── report.md            # generated from report.template.md + results
│   └── figures/
├── proto/relay/v1/*.proto   # generated Go code in gen/relay/v1
├── router/
│   ├── cmd/router/
│   └── internal/
│       ├── api/             # OpenAI-compatible handlers, SSE, request flow
│       ├── routing/         # policies: round-robin, least-loaded, consistent-hash, relay
│       ├── prefix/          # block hashing + per-worker prefix index
│       ├── pool/            # worker membership, health, circuit breakers, sidecar streams
│       ├── admission/       # queueing, backpressure, 429s
│       ├── failover/        # mid-stream recovery
│       ├── worker/          # llama.cpp / vLLM HTTP client + SSE parsing
│       ├── chat/, tokenize/ # chat templates, segment-cached tokenization
│       ├── config/
│       └── telemetry/       # Prometheus metrics, OpenTelemetry tracing
├── sidecar/
├── autoscaler/
├── loadgen/
├── simworker/
├── experiments/
│   ├── workloads/           # workload definitions
│   ├── scenarios/           # one file per experiment
│   ├── harness.py, k8s.py   # local process cluster / Kubernetes target
│   ├── run.py, report.py
│   └── plot.py
├── deploy/
│   ├── docker-compose.yml   # quickest local loop: router + 3 CPU workers
│   ├── helm/relay/
│   ├── k8s/                 # Prometheus / Grafana values for cluster-up
│   ├── chaos/               # Chaos Mesh manifests per failure experiment
│   ├── grafana/dashboards/
│   └── terraform/gpu-bench/
├── scripts/bench-gpu.sh
└── .github/workflows/
```

---

## Router design

### Request flow
1. Accept an OpenAI-style request; apply the model's chat template to produce the full prompt text (the router sends `/v1/completions` to workers internally so failover can continue a partial response — see below).
2. Compute the prompt's **prefix block hashes**.
3. Admission control: if the global queue is full, return `429` with `Retry-After`.
4. Score eligible workers and pick one.
5. Proxy the token stream back to the client, tracking every token emitted.
6. Update the prefix index with the blocks just sent to that worker.

### Prefix block hashing
- The prompt is split into fixed-size blocks (default 64 tokens), tokenized with the worker's `/tokenize` endpoint and cached per chat-template segment.
- Blocks are hashed as a chain: `h_i = hash(h_{i-1} || block_i)`, so a block's hash identifies the entire prefix up to it.
- A request's **match length** on a worker is the number of leading block hashes present in that worker's index.

### Per-worker prefix index
- Each router keeps, per worker, an LRU set of block hashes it believes are cached there, sized to approximate that worker's cache capacity (from the sidecar's reported cache size).
- The index is **soft state**: it's built from the router's own routing decisions and corrected by sidecar reports (cache usage, evictions, restarts). On a worker restart, its index is cleared. Losing the whole index (router restart) only costs cache hits temporarily — never correctness.

### Scoring (the `relay` policy)
For each healthy worker `w`:

```
score(w) = α · match_blocks(w) − β · queue_depth(w) − γ · kv_cache_usage(w)
```

- **Bounded load:** a worker whose in-flight load exceeds `c × average load` (default `c = 1.25`) is ineligible, regardless of cache match. This prevents a hot prefix (e.g. a popular system prompt) from melting one worker.
- Ties broken by fewer in-flight requests, then randomly.
- `α, β, γ, c` are config values; experiments sweep them.

### Baseline policies (same interface, for comparison)
`round_robin`, `least_loaded` (fewest in-flight + queued), `consistent_hash` (hash of the first N blocks onto a ring, no load awareness).

### Worker pool and health
- Membership comes from Kubernetes Endpoints (or a static list in docker compose).
- Active health checks plus passive detection: consecutive errors or stalled streams open a per-worker **circuit breaker** (half-open probing to recover).
- Sidecar stream silence beyond a timeout marks the worker suspect.

### Admission control and backpressure
- A bounded global queue with per-request deadline; requests that would wait past their deadline are rejected early rather than timing out late.
- `429` responses include `Retry-After`; the load generator respects it.

### Mid-stream failover
When a worker dies or stalls (no token within `STALL_TIMEOUT_MS`) during generation:
1. The router already holds the prompt and every token streamed to the client so far.
2. It picks another worker (excluding the failed one) and sends `prompt + generated_so_far` as a completion request, asking for the remaining `max_tokens − emitted`.
3. It continues streaming to the client on the same connection. The client sees a pause, not an error.
4. The **failover pause** (gap between last token before failure and first token after) is recorded as a metric.
5. After `MAX_FAILOVERS` (default 2) per request, it returns a clean error event.

With sampling, the continuation differs from what the dead worker would have produced. With `temperature=0` it matches: `TestMidStreamFailoverOverHTTP` in `router/internal/api/api_test.go` crashes a worker after 3 tokens and checks that the client receives exactly the text an uninterrupted greedy run produces.

### Router high availability
- Two router replicas run behind a Kubernetes Service. Each independently receives sidecar streams and builds its own index — no shared database.
- Killing one router doesn't drop requests on the other; clients retry connection failures. E6 measures the temporary cache-hit dip.

---

## Sidecar

- Scrapes the model server's metrics every 250ms: running requests, waiting requests, KV-cache usage, prefix-cache hit counters (vLLM exposes these as Prometheus metrics; for llama.cpp use its `/metrics` and `/slots` endpoints).
- Streams a compact `WorkerStats` message to every connected router over gRPC.
- Reports restarts (cache cleared) so routers can reset that worker's index.
- On `SIGTERM` (scale-down or node drain): announce `draining` so routers stop sending new requests, wait for in-flight streams to finish (bounded by a grace period), then exit.

---

## Autoscaler

A small Go controller using client-go (written by hand instead of using KEDA):
- It reads cluster-wide queue depth and TTFT p95 from Prometheus.
- Scales the worker Deployment up when queue depth per worker or TTFT p95 exceeds targets for a sustained window; scales down after a longer cooldown, one worker at a time, relying on sidecar draining.
- Min/max replica bounds; logs every decision with its reason; exposes its decisions as metrics.

---

## Workloads

Defined in `experiments/workloads/`; each has a configurable request rate and duration.

| Workload | What it models | Prefix sharing |
|---|---|---|
| `shared-system-prompt` | Many users, a few long system prompts | High, skewed |
| `multi-turn-chat` | Conversations that grow turn by turn (use a public multi-turn chat dataset such as ShareGPT-style data, or synthetic) | High within a conversation |
| `rag-shared-docs` | Questions over a small set of long documents | Medium |
| `unique` | No shared prefixes | None (checks that Relay doesn't lose to the baselines) |
| `hot-prefix` | One prefix receives most traffic | Extreme skew (tests bounded load) |

The load generator records per request: start time, TTFT, inter-token latencies, total tokens, status, failover count.

---

## Experiments

Each experiment lives in `experiments/scenarios/`, runs with `make exp NAME=...`, writes raw results to `results/`, and generates graphs into `docs/report.md`.

| ID | Experiment | Key graph |
|---|---|---|
| E1 | Routing policies across all workloads at fixed load | TTFT p50/p99 and cache hit rate per policy |
| E2 | Load sweep up to saturation | Throughput at a TTFT p99 target ("goodput") per policy |
| E3 | Hot-prefix skew: `consistent_hash` vs `relay` | Per-worker load and TTFT p99 — bounded load prevents a hotspot |
| E4 | Kill a worker mid-stream under load | Failover pause distribution; zero client-visible errors |
| E5 | Network partition of one worker (Chaos Mesh) | Detection time, recovery, error rate |
| E6 | Kill a router replica | Requests dropped (target: none beyond retried connections), cache-hit dip and recovery |
| E7 | Traffic spike with autoscaler on | Worker count, queue depth and TTFT over time |
| E8 | Parameter sweep of α/β/γ/c | Sensitivity of TTFT p99 and hit rate |

E1–E3 were run on the simulated cluster (and E1 on real llama.cpp CPU workers). GPU runs with vLLM are scripted (`make bench-gpu`) but haven't been run yet.

---

## Observability

- Router metrics: requests by policy/status, TTFT and inter-token latency histograms, routing decision match length, queue depth, 429s, circuit-breaker state, failovers and failover pause.
- Worker metrics via sidecar: queue depth, KV-cache usage, prefix-cache hit rate.
- Autoscaler: replica count and decision reasons.
- OpenTelemetry trace per request: admission → routing decision (with scores) → worker → failover if any.
- Grafana dashboard JSON is committed.

Useful dashboard panels per experiment (`deploy/grafana/dashboards/relay.json`):
E1/E2 — *TTFT p50/p99 by policy*, *Prefix-cache hit rate*; E3 — *Worker queue depth*, *Bounded-load exclusions/s*;
E4 — *Failovers/s and pause p99*, *Circuit breaker state*; E5 — *Eligible workers per router*, *Circuit breaker state*;
E6 — *Requests/s by status*, *Prefix-cache hit rate*; E7 — *Autoscaler replicas*, *Admission queue depth*, *TTFT*.

---

## Deployment

- **Fastest loop:** `docker compose up` — one router and three llama.cpp CPU workers with sidecars.
- **Local cluster:** k3s (VirtualBox VMs or k3d) with the Helm chart, Prometheus, Grafana and Chaos Mesh installed via `make cluster-up`.
- **GPU benchmarks:** `deploy/terraform/gpu-bench/` creates GPU VMs, installs k3s, deploys the chart with vLLM workers; `make bench-gpu` runs E1–E3 and copies results back; `make bench-down` destroys everything. A full session is estimated at about $5–10 on one g6.12xlarge spot instance (decisions.md D20).
- Every pod has resource requests/limits, liveness and readiness probes; workers have a termination grace period long enough to drain.

---

## Out of scope

- Disaggregated prefill/decode across workers
- Transferring KV-cache contents between workers
- Multi-model routing or LoRA adapter routing
- Training, fine-tuning, or model-quality evaluation
- Authentication and multi-tenancy beyond a simple API key
