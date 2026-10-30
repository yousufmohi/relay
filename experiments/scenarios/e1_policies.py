"""E1 — every routing policy on every workload at a fixed load.

Each (workload, policy) cell is run with 3 seeds and the requests are pooled:
one 60 s run has ~250 requests, so its p99 rests on 2-3 samples and is too
noisy to compare policies (seed-to-seed p99 varies by ±25%).
"""
import os
import threading

from harness import Cluster, log
from plot import rdir, COLORS, POLICIES, VARIANTS, grouped_bars, load, load_many, md_table, plt, save, summarize, write_section

WORKLOADS = ["shared-system-prompt", "multi-turn-chat", "rag-shared-docs", "unique", "hot-prefix"]
SHORT = ["shared-sys", "multi-turn", "rag-docs", "unique", "hot-prefix"]
WARMUP = 10.0
SEEDS = (1, 2, 3)
ALL = POLICIES + list(VARIANTS)


def files(wl: str, pol: str) -> list:
    return sorted(rdir("e1").glob(f"{wl}__{pol}__s*.jsonl"))


def run(quick: bool = False) -> None:
    dur = "25s" if quick else "60s"
    local = os.environ.get("RELAY_TARGET") != "k8s"

    def one(wl, pol, seed):
        if (rdir("e1") / f"{wl}__{pol}__s{seed}.jsonl").exists():
            return  # resume: keep finished runs
        # Fresh cluster per run: every policy starts with cold caches. Local
        # seeds run side by side on separate ports.
        kw = {"base_port": 30000 + 2000 * seed} if local else {}
        policy, env = VARIANTS.get(pol, (pol, {}))
        with Cluster("e1", workers=4, policy=policy, router_env=env, **kw) as c:
            c.loadgen(wl, f"{wl}__{pol}__s{seed}.jsonl", duration=dur, extra=["-seed", str(seed)])

    for wl in WORKLOADS:
        for pol in ALL:
            log(f"E1 {wl} / {pol}")
            if local:
                ts = [threading.Thread(target=one, args=(wl, pol, s)) for s in SEEDS]
                [t.start() for t in ts]
                [t.join() for t in ts]
            else:
                for s in SEEDS:
                    one(wl, pol, s)


def analyze() -> None:
    rows, vals = [], {}
    for wl, short in zip(WORKLOADS, SHORT):
        for pol in ALL:
            fs = files(wl, pol)
            if not fs:
                continue
            s = summarize(load_many(fs), warmup=WARMUP)
            p99s = [summarize(load(f), warmup=WARMUP)["ttft_p99"] for f in fs]
            s["p99_range"] = f"{min(p99s):.0f}–{max(p99s):.0f}"
            s["seeds"] = len(fs)
            s["throughput"] /= len(fs)  # pooled seeds ran side by side
            rows.append({"workload": wl, "policy": pol, **s})
            for k in ("ttft_p50", "ttft_p99", "hit_rate"):
                vals[(k, short, pol)] = s[k]

    fig, axes = plt.subplots(3, 1, figsize=(11, 10))
    for ax, (key, label, fmt) in zip(axes, [("ttft_p50", "TTFT p50 (ms)", "{:.0f}"), ("ttft_p99", "TTFT p99 (ms)", "{:.0f}"),
                                            ("hit_rate", "Prefix-cache hit rate", "{:.2f}")]):
        pols = [p for p in ALL if any((key, s, p) in vals for s in SHORT)]
        grouped_bars(ax, SHORT, pols, {(s, p): vals.get((key, s, p), float("nan")) for s in SHORT for p in pols},
                     COLORS, label, fmt)
        ax.set_title(label + " by workload and policy")
        if key.startswith("ttft"):
            # consistent_hash's hotspot tails (tens of seconds) would flatten
            # every other difference on a linear axis.
            ax.set_yscale("log")
            ax.set_ylabel(label.replace("(ms)", "(ms, log scale)"))
    axes[0].legend(ncols=5, loc="lower left", bbox_to_anchor=(0, 1.08))
    f = save(fig, "e1_policies")

    table = md_table(rows, [("workload", "workload", ""), ("policy", "policy", ""), ("ttft_p50", "TTFT p50 ms", ".0f"),
                            ("ttft_p99", "TTFT p99 ms (pooled)", ".0f"), ("p99_range", "p99 range across seeds", ""),
                            ("hit_rate", "hit rate", ".3f"),
                            ("throughput", "req/s", ".2f"), ("errors", "errors", "d")])
    write_section("e1", f"""
## E1 — Routing policies across workloads (fixed load)

4 simulated workers (4 slots, 24k-token prefix cache each), one router, open-loop Poisson arrivals at each
workload's default rate, {WARMUP:.0f}s warm-up excluded. Each run starts from cold caches; every cell pools 3 seeds
(~750 requests) because single-run p99s are too noisy to rank policies. `relay_floor4` is relay with
`LOAD_BOUND_FLOOR=4` (the workers' slot count), added after E8 suggested it.

![E1]({f})

{table}
""")
