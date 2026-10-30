## E1 on real models — llama.cpp CPU workers

The docker-compose stack: router + 3 llama.cpp workers (Qwen2.5-0.5B-Instruct Q4_K_M, 4 slots, 2 CPU threads each)
with sidecars, `shared-system-prompt` at 0.4 req/s for 120s with 16-token answers (CPU prefill
of a ~1.3k-token system prompt takes seconds, so the load is light). Hit rate comes from llama.cpp's own
`timings.cache_n`. One run per policy, so treat p99 (a handful of requests) as indicative only.

![E1 llama.cpp](figures/e1_llamacpp.png)

| policy | requests | TTFT p50 ms | TTFT p99 ms | hit rate (llama.cpp cache_n) | errors |
|---|---:|---:|---:|---:|---:|
| round_robin | 37 | 450 | 7408 | 0.489 | 0 |
| least_loaded | 37 | 320 | 4116 | 0.827 | 0 |
| consistent_hash | 37 | 328 | 3527 | 0.900 | 0 |
| relay | 37 | 293 | 2492 | 0.929 | 0 |
