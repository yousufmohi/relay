# Design decisions

Decisions not fixed by the original design ([design.md](design.md)), one short entry each.

### D1. Single Go module, generated code committed
One `go.mod` at the root for router, sidecar, autoscaler, loadgen and simworker; protobuf output lives in
`gen/relay/v1` and is committed, so building needs no `protoc`. `buf generate` (`make proto`) regenerates it.

### D2. Routers dial sidecars (sidecar is the gRPC server)
Each router opens one bidirectional `Watch` stream per sidecar it discovers. That keeps sidecars stateless about who
the routers are, makes "every router builds its own view" automatic, and router replicas can come and go without
re-configuring workers. Message names follow `buf lint` (`WatchRequest`/`WatchResponse`).

### D3. Membership from the headless Service's DNS, not the Endpoints API
Routers resolve `<release>-worker-headless` every 2 s; its A records are exactly the ready pod IPs from the
Endpoints object. No client-go or RBAC in the router. Static lists (`WORKERS`) and a polled file (`WORKERS_FILE`, used
by the local experiment harness to add/remove workers) are the alternatives.

### D4. llama.cpp via its native `/completion` endpoint
The router always sends completion (not chat) requests. For llama.cpp it uses the native `/completion` stream because
its final event reports `timings.cache_n` (prompt tokens served from the slot cache), giving an exact per-request
cache hit measurement. For vLLM it uses `/v1/completions` with `stream_options.include_usage` and
`--enable-prompt-tokens-details`, which reports `usage.prompt_tokens_details.cached_tokens`.

### D5. Chat template rendered as segments; tokenization cached per segment
The chat template is rendered as a list of segments (role header, content, footer). The remote tokenizer
(`TOKENIZER=remote`, the worker's `/tokenize`) is called once per distinct segment and cached (LRU, 100k entries), so
a shared system prompt or earlier turn is tokenized once. Special tokens at segment boundaries make per-segment
tokenization equal to whole-prompt tokenization. `TOKENIZER=approx` (4 bytes per pseudo-token, no network) is the
fallback, and is used automatically if `/tokenize` fails.

### D6. Load = max(own in-flight, sidecar running+waiting)
The router's own in-flight count is exact and instant but blind to other replicas; the sidecar's count sees all
replicas but lags by up to one scrape interval. Taking the max is right for one router and safe for several.
`queue_depth(w)` in the score and the bounded-load test both use this number.

### D7. Bounded load uses the Mirrokni et al. form
A worker is eligible if `load + 1 <= ceil(c · (total_load + 1) / n)`. The "+1"s make an idle cluster admit
everyone, and the least-loaded worker always satisfies the bound, so the eligible set is never empty.

### D8. Prefix index updated at decision time
Blocks are inserted into the chosen worker's index when the decision is made, not when the response completes, so
concurrent requests with the same prefix immediately see it. Generated tokens are not inserted: the next turn's
prompt re-renders the assistant message as a new segment, and the router does not tokenize responses.
On LRU insert, blocks are touched last-to-first so a prompt's head is the most recently used part (a cached tail
is useless without its head).

### D9. Separate first-token timeout from the stall timeout
`STALL_TIMEOUT_MS` (default 3 s) applies between tokens. Before the first token a healthy but busy worker sends
nothing while the request is queued or prefilling, so a separate `FIRST_TOKEN_TIMEOUT_MS` (default 30 s) applies.
An early version used one timer for both and failed over (and tripped breakers on) workers that were merely busy;
E3 caught it. Dead workers are still detected quickly before their first token through the pool (see D10).

### D10. Streams poll the pool for "worker down"
While a stream runs, the failover loop asks the pool every 250 ms whether the worker is down (failed health check,
sidecar silent for > `SIDECAR_TIMEOUT_MS`, or breaker open). A partitioned worker's streams are abandoned as soon
as any detector fires instead of waiting for a timeout. Draining is explicitly *not* down.

### D11. Workers that leave membership linger until idle
A draining pod fails readiness and disappears from the headless Service while it still serves streams. The pool
keeps such a worker (ineligible for new requests, not "down") until its in-flight count reaches zero, then purges
it. Without this, scale-down would abort the very streams draining is meant to protect.

### D12. Graceful drain via preStop hooks calling the sidecar
On scale-down Kubernetes runs each container's preStop before sending SIGTERM. The model container's preStop calls
the sidecar's `POST /drain`, which announces `DRAINING` to routers immediately and blocks until the model server
reports zero running/waiting requests on two consecutive scrapes (or `DRAIN_GRACE_S`). The model server therefore
keeps running until its streams finish. `terminationGracePeriodSeconds` (180 s) exceeds the drain grace (150 s).

### D13. Restart detection by epoch
The sidecar increments an `epoch` when the model server's cumulative counters go backwards, its process start time
changes, or it comes back after being unreachable. Routers clear a worker's prefix index when `(instance_id, epoch)`
changes. A sidecar restart (new `instance_id`) clears it too: conservative, costs only cache hits.

### D14. Admission: global concurrency limit + FIFO queue + early rejection
Capacity = eligible workers × `MAX_INFLIGHT_PER_WORKER` (default 8). Beyond it requests wait in a FIFO queue of
`QUEUE_MAX`. The expected wait is `service_time_ewma × (position + 1) / capacity`; if that exceeds the request's
deadline (`X-Relay-Deadline-Ms` header, default `QUEUE_DEADLINE_MS` = 30 s) it is rejected immediately with 429 and
`Retry-After` = the estimate. With zero capacity the answer is 503. Known limitation: one global queue means a
hotspot worker can hold most slots and delay requests destined for idle workers (visible for `consistent_hash` in E3).
Per-worker queues would avoid that; out of scope.

### D15. Failover usage accounting
`prompt_tokens`/`cached_tokens` reported to the client come from the first attempt (the routing decision being
evaluated); `completion_tokens` counts tokens emitted across attempts. Tokens are counted as non-empty stream chunks
(llama.cpp and vLLM stream one token per chunk by default).

### D16. Simulated workers for laptop-scale experiments
`simworker` emulates llama.cpp's HTTP API with a block-based LRU prefix cache, a fixed number of slots, prefill cost
proportional to *uncached* tokens and decode cost growing with batch size. Its output is a deterministic function of
the context (a stand-in for temperature-0 decoding), which lets tests check failover continuations for coherence.
Parameters (default: 4 slots, 24k-token cache, 0.4 ms/uncached prompt token, 20 ms/decode step, +15 % per extra
sequence) are roughly a 7B model on one mid-range GPU. All local experiment numbers are from simworker; the report
labels them as such. Real-model runs: `docker compose` (llama.cpp, CPU) and `make bench-gpu` (vLLM).

### D17. Local experiment harness instead of k3s for E1–E8 development
`experiments/harness.py` runs workers, sidecars and routers as local processes and injects failures directly
(SIGKILL, freezing a worker + silencing its sidecar for partitions, the sidecar's `/drain` for scale-down). The same
scenario code runs against Kubernetes with `RELAY_TARGET=k8s` (`experiments/k8s.py`), where failures come from Chaos
Mesh. Results, figures and report sections are kept separate per target (`-k8s`, `-gpu` suffixes).

### D18. The autoscaler's local backend is a file
The autoscaler scales a Deployment through the `/scale` subresource. For the local E7 it can instead write the
desired count to `SCALE_FILE`; the harness reconciles workers to it and writes the actual count back. Its metric
source is Prometheus in-cluster, or a direct scrape of the routers' `/metrics` (computing TTFT p95 from histogram
bucket deltas the same way `histogram_quantile` does) when no Prometheus exists.

### D19. Autoscaler policy
Scale up when queue-per-worker or TTFT p95 exceeds target continuously for `AUTOSCALER_UP_WINDOW` (20 s), by
`ceil(replicas × min(ratio, 2))` (at least +1, at most doubling), then wait `AUTOSCALER_UP_COOLDOWN`. Scale down
one replica at a time when both signals are below half their targets for `AUTOSCALER_DOWN_WINDOW` (90 s), and never
within that window after any scaling action (prevents flapping).

### D20. GPU benchmark on one 4×L4 spot VM
`deploy/terraform/gpu-bench` creates a single AWS `g6.12xlarge` spot instance (4× L4 24 GB) running k3s with the NVIDIA
device plugin: four vLLM workers with one GPU each (Qwen2.5-7B-Instruct, bf16, `--enable-prefix-caching`). Spot
price is typically $1.5–2.5/h; a full E1+E2+E3+E8 session (~2.5 h including model download) is ≈ $5–10, far under
the $50 target. Rates are scaled with `RELAY_RATE_SCALE` because real GPUs are faster than the simulated workers.

### D21. Spec defaults kept; tuned settings recommended from data
E8 and the E1 follow-ups found two settings better than the spec defaults on the simulated cluster:
`LOAD_BOUND_FLOOR=<worker slots>` (the plain bounded-load rule is very strict at small integer loads) and
`SCORE_GAMMA=0` (the KV-usage term, fed by sidecar data up to one scrape old, slightly worsened p99 on `unique`).
The code keeps the documented defaults (`LOAD_BOUND_FLOOR=0`, `SCORE_GAMMA=0.5`) so behaviour matches the spec; the report
documents the recommended values and the evidence. They should be re-checked on GPU/vLLM before changing defaults.

### D22. Local harness ports stay below 49152
Windows assigns ephemeral ports from 49152 up; a harness cluster placed there failed to bind. All harness port
ranges are in 10000–46000.
