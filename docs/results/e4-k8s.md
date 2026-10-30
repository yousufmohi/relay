## E4 — Killing workers mid-stream (k8s)

`relay` policy, 4 workers, `shared-system-prompt` at 3 req/s with 120-token responses (long streams, so the kill lands
mid-generation). Rolling crashes: every 10 s from 15 s one worker is killed (SIGKILL, no drain) and restarted 5 s
later with an empty cache, so at most one of four workers is down at a time.

![E4](figures/e4_worker_kill-k8s.png)

| requests | client-visible errors | requests that failed over | failovers | pause p50 ms | pause p99 ms | pause max ms |
|---:|---:|---:|---:|---:|---:|---:|
| 220 | 0 | 13 | 13 | 655 | 2007 | 2050 |

| kill | streams failed over | pause p50 ms | pause max ms |
|---|---:|---:|---:|
| w1 at 15s | 1 | 167 | 167 |
| w2 at 25s | 4 | 147 | 1624 |
| w3 at 35s | 2 | 455 | 751 |
| w0 at 45s | 1 | 1049 | 1049 |
| w1 at 55s | 3 | 1661 | 2050 |

Every request that was streaming on a killed worker was continued on another worker on the same client connection;
the client sees a pause, not an error. The pause is dominated by re-prefilling `prompt + generated_so_far` on the new
worker (the dead worker's KV cache is lost) plus any wait for a free slot there. In an earlier run with a second kill
(2 of 4 workers left, 200-token streams) the survivors were overloaded and pauses grew to 4–6 s of queueing: failover
keeps streams alive but cannot create capacity.
