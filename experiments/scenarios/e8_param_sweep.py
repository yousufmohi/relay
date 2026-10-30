"""E8 — sensitivity of the relay policy to α, β, γ, c and the load-bound floor
(one parameter at a time around the defaults)."""
import os
import threading

from harness import Cluster, log
from plot import rdir, INK2, SERIES, load, md_table, plt, save, summarize, write_section

DEFAULT = {"SCORE_ALPHA": 1.0, "SCORE_BETA": 0.5, "SCORE_GAMMA": 0.5, "LOAD_BOUND_C": 1.25, "LOAD_BOUND_FLOOR": 0}
SWEEP = {
    "SCORE_ALPHA": [0.1, 0.25, 1.0, 4.0],
    "SCORE_BETA": [0.0, 0.25, 0.5, 2.0],
    "SCORE_GAMMA": [0.0, 0.5, 4.0],
    "LOAD_BOUND_C": [1.0, 1.1, 1.25, 1.5, 2.0, 100.0],
    "LOAD_BOUND_FLOOR": [0, 2, 4, 8],
}
LABEL = {"SCORE_ALPHA": "α (cache match weight)", "SCORE_BETA": "β (queue weight)", "SCORE_GAMMA": "γ (KV usage weight)",
         "LOAD_BOUND_C": "c (load bound; 100 = off)", "LOAD_BOUND_FLOOR": "load-bound floor (0 = off)"}
WORKLOADS = {"shared-system-prompt": SERIES[4], "multi-turn-chat": SERIES[5], "hot-prefix": SERIES[6]}
RATE = {"shared-system-prompt": 8, "multi-turn-chat": 6, "hot-prefix": 8}  # near saturation
WARMUP = 8.0


def configs():
    seen = set()
    for param, values in SWEEP.items():
        for v in values:
            cfg = dict(DEFAULT, **{param: v})
            key = tuple(sorted(cfg.items()))
            if key not in seen:
                seen.add(key)
                yield cfg


def name(cfg: dict) -> str:
    return "_".join(f"{k.split('_')[-1].lower()}{v:g}" for k, v in sorted(cfg.items()))


def run(quick: bool = False) -> None:
    local = os.environ.get("RELAY_TARGET") != "k8s"

    def sweep(i, wl):
        for cfg in configs():
            if (rdir("e8") / f"{wl}__{name(cfg)}.jsonl").exists():
                continue  # resume: keep finished runs
            log(f"E8 {wl} {name(cfg)}")
            kw = {"base_port": 40000 + 2000 * i} if local else {}
            with Cluster("e8", workers=4, policy="relay", router_env=cfg, **kw) as c:
                c.loadgen(wl, f"{wl}__{name(cfg)}.jsonl", rate=RATE[wl], duration="20s" if quick else "40s")

    if local:  # one workload per thread, separate ports
        ts = [threading.Thread(target=sweep, args=(i, wl)) for i, wl in enumerate(WORKLOADS)]
        [t.start() for t in ts]
        [t.join() for t in ts]
    else:
        for i, wl in enumerate(WORKLOADS):
            sweep(i, wl)


def analyze() -> None:
    rows = []
    for wl in WORKLOADS:
        for cfg in configs():
            p = rdir("e8") / f"{wl}__{name(cfg)}.jsonl"
            if p.exists():
                rows.append({"workload": wl, **cfg, **summarize(load(p), warmup=WARMUP)})
    if not rows:
        return
    fig, axes = plt.subplots(2, len(SWEEP), figsize=(18, 6.5))
    for j, param in enumerate(SWEEP):
        for wl, color in WORKLOADS.items():
            pts = sorted((r[param], r) for r in rows if r["workload"] == wl and
                         all(r[k] == v for k, v in DEFAULT.items() if k != param))
            if not pts:
                continue
            xs = list(range(len(pts)))
            axes[0][j].plot(xs, [r["ttft_p99"] for _, r in pts], marker="o", markersize=5, color=color, label=wl)
            axes[1][j].plot(xs, [r["hit_rate"] for _, r in pts], marker="o", markersize=5, color=color, label=wl)
            for ax in (axes[0][j], axes[1][j]):
                ax.set_xticks(xs, [f"{v:g}" for v, _ in pts])
                d = [i for i, (v, _) in enumerate(pts) if v == DEFAULT[param]]
                if d:
                    ax.axvline(d[0], color=INK2, linestyle=":", linewidth=1)
        axes[0][j].set(title=LABEL[param], yscale="log")
        axes[1][j].set(xlabel=LABEL[param], ylim=(0, 1))
    axes[0][0].set_ylabel("TTFT p99 (ms, log scale)")
    axes[1][0].set_ylabel("prefix-cache hit rate")
    axes[0][0].legend(fontsize=8, loc="upper left")
    f = save(fig, "e8_param_sweep")
    t = md_table(rows, [("workload", "workload", ""), ("SCORE_ALPHA", "α", "g"), ("SCORE_BETA", "β", "g"),
                        ("SCORE_GAMMA", "γ", "g"), ("LOAD_BOUND_C", "c", "g"), ("LOAD_BOUND_FLOOR", "floor", "g"), ("ttft_p50", "p50 ms", ".0f"),
                        ("ttft_p99", "p99 ms", ".0f"), ("hit_rate", "hit rate", ".3f"), ("errors", "errors", "d")])
    write_section("e8", f"""
## E8 — Parameter sensitivity (α, β, γ, c)

One parameter varied at a time around the defaults (α=1, β=0.5, γ=0.5, c=1.25, floor 0; dotted line), 4 workers,
near saturation where the load terms matter (8 req/s; multi-turn 6 req/s).

![E8]({f})

<details><summary>All runs</summary>

{t}

</details>
""")
