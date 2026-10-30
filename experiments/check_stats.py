"""M2 acceptance check: the router's view of each worker's queue depth and KV
cache usage matches the worker's own metrics within one scrape interval.

Under load, samples every worker's /metrics and the router's /debug/workers
at ~40 ms. A router reading at time t is correct if the same value appeared in
the worker's own metrics at some point in [t - scrape_interval - slack, t].

    python experiments/check_stats.py
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import json  # noqa: E402

from harness import Cluster, http_get, log  # noqa: E402

SCRAPE_S = 0.25
SLACK_S = 0.06  # sampling granularity + HTTP latency


def parse(text: str) -> dict:
    out = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            k, v = line.split()[:2]
            out[k] = float(v)
    return out


def main() -> int:
    with Cluster("m2check", workers=4, policy="relay") as c:
        worker_series = {w.id: [] for w in c.live_workers()}
        router_series = []
        stop = threading.Event()

        def poll_worker(w):
            while not stop.is_set():
                t = time.time()
                try:
                    m = parse(http_get(w.url + "/metrics", 1))
                    worker_series[w.id].append((t, int(m["llamacpp:requests_processing"]),
                                                int(m["llamacpp:requests_deferred"]), m["llamacpp:kv_cache_usage_ratio"]))
                except Exception:
                    pass
                time.sleep(0.04)

        def poll_router():
            while not stop.is_set():
                t = time.time()
                try:
                    d = json.loads(http_get(c.router_urls[0] + "/debug/workers", 1))
                    router_series.append((t, d["workers"]))
                except Exception:
                    pass
                time.sleep(0.04)

        ths = [threading.Thread(target=poll_worker, args=(w,)) for w in c.live_workers()]
        ths.append(threading.Thread(target=poll_router))
        [t.start() for t in ths]
        c.loadgen("shared-system-prompt", "load.jsonl", rate=8, duration="30s")
        stop.set()
        [t.join() for t in ths]

    checked = matched = 0
    for t, workers in router_series:
        for w in workers:
            series = worker_series.get(w["ID"], [])
            window = [s for s in series if t - SCRAPE_S - SLACK_S <= s[0] <= t]
            if not window:
                continue
            checked += 1
            if any(s[1] == w["Running"] and s[2] == w["Waiting"] and abs(s[3] - w["KVUsage"]) < 1e-3 for s in window):
                matched += 1
    rate = matched / max(1, checked)
    log(f"M2 check: {matched}/{checked} router readings ({rate:.1%}) match a worker sample within one scrape interval")
    out = Path(__file__).resolve().parent.parent / "docs" / "results" / "m2.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        f"## M2 — Router view vs worker metrics\n\n"
        f"Under load (8 req/s, 4 workers), {checked} router readings of (running, waiting, KV usage) were compared with "
        f"the workers' own `/metrics` sampled every ~40 ms: **{rate:.1%}** matched a worker sample from within one "
        f"scrape interval ({SCRAPE_S * 1000:.0f} ms, +{SLACK_S * 1000:.0f} ms sampling slack). "
        f"Reproduce with `python experiments/check_stats.py`.\n", encoding="utf-8")
    return 0 if rate >= 0.95 else 1


if __name__ == "__main__":
    sys.exit(main())
