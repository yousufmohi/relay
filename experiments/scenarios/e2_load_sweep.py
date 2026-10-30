"""E2 — load sweep to saturation; goodput at a TTFT p99 target."""
from harness import Cluster, log
from plot import rdir, COLORS, INK2, POLICIES, load, md_table, plt, save, summarize, write_section

WORKLOAD = "shared-system-prompt"
RATES = [2, 4, 6, 8, 10, 12, 14]
SLO_MS = 1000.0
WARMUP = 8.0


def run(quick: bool = False) -> None:
    rates = RATES[::2] if quick else RATES
    dur = "20s" if quick else "40s"
    for pol in POLICIES:
        for r in rates:
            log(f"E2 {pol} @ {r} req/s")
            with Cluster("e2", workers=4, policy=pol) as c:
                c.loadgen(WORKLOAD, f"{pol}__{r}.jsonl", rate=r, duration=dur)


def analyze() -> None:
    rows = []
    for pol in POLICIES:
        for r in RATES:
            p = rdir("e2") / f"{pol}__{r}.jsonl"
            if p.exists():
                rows.append({"policy": pol, "rate": r, **summarize(load(p), warmup=WARMUP, slo_ms=SLO_MS)})

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.5))
    best = []
    for pol in POLICIES:
        pr = [x for x in rows if x["policy"] == pol]
        if not pr:
            continue
        xs = [x["rate"] for x in pr]
        a1.plot(xs, [x["ttft_p99"] for x in pr], marker="o", markersize=5, color=COLORS[pol], label=pol)
        a2.plot(xs, [x["goodput"] for x in pr], marker="o", markersize=5, color=COLORS[pol], label=pol)
        # Max sustainable load: highest offered rate meeting the SLO with <1% errors.
        okr = [x["rate"] for x in pr if x["ttft_p99"] <= SLO_MS and x["error_rate"] < 0.01]
        best.append({"policy": pol, "max_rate": max(okr) if okr else 0, "peak_goodput": max(x["goodput"] for x in pr)})
    a1.axhline(SLO_MS, color=INK2, linestyle="--", linewidth=1)
    a1.annotate(f"target p99 ≤ {SLO_MS:.0f} ms", (RATES[0], SLO_MS), xytext=(0, 4), textcoords="offset points", color=INK2, fontsize=8)
    a1.set(yscale="log", xlabel="offered load (req/s)", ylabel="TTFT p99 (ms, log scale)", title="TTFT p99 vs offered load")
    a2.set(xlabel="offered load (req/s)", ylabel=f"goodput (req/s with TTFT ≤ {SLO_MS:.0f} ms)", title="Goodput vs offered load")
    a1.legend()
    f = save(fig, "e2_load_sweep")
    t1 = md_table(best, [("policy", "policy", ""), ("max_rate", f"max load meeting p99 ≤ {SLO_MS:.0f} ms (req/s)", "d"),
                         ("peak_goodput", "peak goodput (req/s)", ".2f")])
    t2 = md_table(rows, [("policy", "policy", ""), ("rate", "offered req/s", "d"), ("ttft_p50", "p50 ms", ".0f"),
                         ("ttft_p99", "p99 ms", ".0f"), ("throughput", "req/s", ".2f"), ("goodput", "goodput", ".2f"),
                         ("hit_rate", "hit rate", ".3f"), ("error_rate", "err rate", ".3f")])
    write_section("e2", f"""
## E2 — Load sweep to saturation (goodput)

Workload `{WORKLOAD}`, 4 workers, each point a fresh cluster. Goodput = completed requests per second whose TTFT met
the {SLO_MS:.0f} ms target.

![E2]({f})

{t1}

<details><summary>All runs</summary>

{t2}

</details>
""")
