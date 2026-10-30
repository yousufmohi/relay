"""Follow-up to E1: relay with γ=0 on the `unique` workload, same seeds as E1.

On `unique` relay matched least_loaded on mean/p90/p95 but had a higher p99 in
all 3 paired seeds. With no prefix matches the only difference from
least_loaded is the γ·kv_usage term (sidecar data, up to one scrape stale);
this reruns relay with SCORE_GAMMA=0 to test that.

    python experiments/check_gamma0.py
"""
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import Cluster  # noqa: E402
from plot import load, rdir  # noqa: E402

res = {}


def one(seed: int) -> None:
    with Cluster("e1-gamma0", workers=4, policy="relay", router_env={"SCORE_GAMMA": "0"}, base_port=10000 + 1000 * seed) as c:
        c.loadgen("unique", f"unique__relay_gamma0__s{seed}.jsonl", duration="60s", extra=["-seed", str(seed)])


def p99(path: Path) -> float:
    d = load(path)
    d = d[(d.t_start >= 10) & d.ok]
    return float(d.ttft_ms.quantile(0.99))


if __name__ == "__main__":
    ts = [threading.Thread(target=one, args=(s,)) for s in (1, 2, 3)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    for s in (1, 2, 3):
        row = [f"seed {s}"]
        for name, d in (("least_loaded", "e1"), ("relay", "e1"), ("relay_gamma0", "e1-gamma0")):
            row.append(f"{name}={p99(rdir(d) / f'unique__{name}__s{s}.jsonl'):.0f}")
        print("  ".join(row))
