"""E3 — hot-prefix skew: consistent_hash vs relay (bounded load)."""
import numpy as np

from harness import Cluster, log
from plot import rdir, COLORS, INK2, SURFACE, load, md_table, plt, save, summarize, write_section

POLICIES = ["consistent_hash", "relay"]
WORKERS = [f"w{i}" for i in range(4)]
WARMUP = 10.0


def run(quick: bool = False) -> None:
    for pol in POLICIES:
        log(f"E3 {pol}")
        with Cluster("e3", workers=4, policy=pol) as c:
            c.loadgen("hot-prefix", f"{pol}.jsonl", rate=8, duration="25s" if quick else "60s")


def analyze() -> None:
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2))
    rows, share_rows = [], []
    for k, pol in enumerate(POLICIES):
        p = rdir("e3") / f"{pol}.jsonl"
        if not p.exists():
            continue
        df = load(p)
        d = df[(df.t_start >= WARMUP) & df.ok]
        share = d.groupby("worker").size().reindex(WORKERS, fill_value=0)
        share = share / max(1, share.sum())
        rows.append({"policy": pol, **summarize(df, warmup=WARMUP), "max_worker_share": share.max()})
        share_rows.append({"policy": pol, **{w: share[w] for w in WORKERS}})
        x = np.arange(len(WORKERS)) + (k - 0.5) * 0.38
        bars = axes[0].bar(x, share.values, 0.38, color=COLORS[pol], label=pol, edgecolor=SURFACE, linewidth=1.5)
        for b, v in zip(bars, share.values):
            axes[0].annotate(f"{v:.0%}", (b.get_x() + b.get_width() / 2, v), ha="center", va="bottom", fontsize=7,
                             color=INK2, xytext=(0, 2), textcoords="offset points")
        t = np.sort(d.ttft_ms.values)
        axes[1].plot(t, np.arange(1, len(t) + 1) / max(1, len(t)), color=COLORS[pol], label=pol)
        p99 = d.groupby("worker").ttft_ms.quantile(0.99).reindex(WORKERS)
        axes[2].bar(x, p99.values, 0.38, color=COLORS[pol], label=pol, edgecolor=SURFACE, linewidth=1.5)
    axes[0].axhline(0.25, color=INK2, linestyle="--", linewidth=1)
    axes[0].annotate("even share", (-0.45, 0.25), xytext=(0, 3), textcoords="offset points", fontsize=8, color=INK2)
    axes[0].set(xticks=range(4), xticklabels=WORKERS, ylabel="share of requests", title="Requests per worker")
    axes[1].set(xscale="log", xlabel="TTFT (ms, log scale)", ylabel="fraction of requests", title="TTFT CDF")
    axes[2].set(xticks=range(4), xticklabels=WORKERS, ylabel="TTFT p99 (ms)", title="TTFT p99 per worker")
    for a in (axes[0], axes[2]):
        a.grid(axis="x", visible=False)
    axes[1].legend(loc="lower right")
    f = save(fig, "e3_hot_prefix")
    t = md_table(rows, [("policy", "policy", ""), ("ttft_p50", "TTFT p50 ms", ".0f"), ("ttft_p99", "TTFT p99 ms", ".0f"),
                        ("hit_rate", "hit rate", ".3f"), ("max_worker_share", "busiest worker share", ".2f"),
                        ("errors", "errors", "d")])
    t2 = md_table(share_rows, [("policy", "policy", "")] + [(w, w, ".2f") for w in WORKERS])
    write_section("e3", f"""
## E3 — Hot-prefix skew: bounded load prevents a hotspot

Workload `hot-prefix` (90% of requests share one 2000-token prefix) at 8 req/s on 4 workers.

![E3]({f})

{t}

Request share per worker:

{t2}
""")
