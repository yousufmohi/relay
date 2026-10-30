"""E5 — network partition of one worker: detection time, recovery, error rate.

Locally the partition is emulated by freezing the simulated worker (no
tokens, no answers to health checks) and silencing its sidecar stream, which
is what a partition looks like from the router. On Kubernetes the same
scenario uses deploy/chaos/e5-partition.yaml (Chaos Mesh NetworkChaos).
"""
import threading
import time

import numpy as np
import pandas as pd

from harness import Cluster, Monitor, log
from plot import rdir, COLORS, INK2, SURFACE, load, md_table, plt, save, summarize, timeline, write_section

PART_AT, HEAL_AT, VICTIM = 20.0, 40.0, 1


def run(quick: bool = False) -> None:
    with Cluster("e5", workers=4, policy="relay") as c, Monitor(c, interval=0.1) as m:
        lg = c.loadgen("shared-system-prompt", "run.jsonl", rate=3, duration="60s", background=True,
                       extra=["-max-tokens", "200"])
        t0 = time.time()

        def chaos():
            time.sleep(PART_AT)
            log("E5 partition w1")
            c.partition_worker(VICTIM, True)
            time.sleep(max(0, HEAL_AT - (time.time() - t0)))
            log("E5 heal w1")
            c.partition_worker(VICTIM, False)

        th = threading.Thread(target=chaos)
        th.start()
        lg.wait()
        th.join()
        m.save(c.dir / "monitor.csv")


def analyze() -> None:
    p = rdir("e5") / "run.jsonl"
    if not p.exists():
        return
    df = load(p)
    mon = pd.read_csv(rdir("e5") / "monitor.csv")
    col = f"ok_w{VICTIM}"
    after = mon[(mon.t >= PART_AT) & (mon[col] == 0)]
    detect = float(after.t.min() - PART_AT) if len(after) else float("nan")
    back = mon[(mon.t >= HEAL_AT) & (mon[col] == 1)]
    recover = float(back.t.min() - HEAL_AT) if len(back) else float("nan")
    s = summarize(df)
    pauses = np.array([x for xs in df["pause_ms"].dropna() for x in (xs if isinstance(xs, list) else [])])
    tl = timeline(df, 2.0)

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6), sharex=True, gridspec_kw={"height_ratios": [1, 2]})
    a1.step(mon.t, mon.eligible, where="post", color=COLORS["relay"])
    a1.set(ylabel="eligible workers", title="Router's view of the pool", ylim=(0, 4.5))
    for ax in (a1, a2):
        ax.axvspan(PART_AT, HEAL_AT, color="#e4e3df", alpha=0.6, linewidth=0)
    a1.annotate(f"partition (detected in {detect:.2f}s)", (PART_AT, 0.3), fontsize=8, color=INK2, xytext=(4, 0),
                textcoords="offset points")
    a2.plot(tl.b, tl.p50, color=COLORS["relay"], label="TTFT p50")
    a2.plot(tl.b, tl.p99, color=COLORS["relay"], linestyle="--", linewidth=1.5, label="TTFT p99")
    fo = df[df.failovers > 0]
    a2.scatter(fo.t_start, np.full(len(fo), max(1, np.nanmin(tl.p50) * 0.6)), marker="v", s=40, color="#e34948",
               label="request that failed over", zorder=3)
    a2.set(yscale="log", xlabel="time (s)", ylabel="TTFT (ms, log scale)", title="Client latency")
    a2.legend(loc="upper left", fontsize=8)
    f = save(fig, "e5_partition")
    row = {**s, "detect": detect, "recover": recover, "pause_max": float(pauses.max()) if len(pauses) else float("nan")}
    t = md_table([row], [("requests", "requests", "d"), ("errors", "client-visible errors", "d"),
                         ("error_rate", "error rate", ".3f"), ("failovers", "failovers", "d"),
                         ("detect", "detection time (s)", ".2f"), ("recover", "time to re-admit after heal (s)", ".2f"),
                         ("pause_max", "max failover pause (ms)", ".0f")])
    write_section("e5", f"""
## E5 — Network partition of one worker

Worker w1 is partitioned from 20 s to 40 s (its model server stops responding and its sidecar stream goes silent).
Detection uses three independent signals, whichever fires first: sidecar silence (> `SIDECAR_TIMEOUT_MS` = 1.5 s),
failed active health checks (every 0.5 s here), and the per-stream stall timer (`STALL_TIMEOUT_MS` = 1.5 s here).
In-flight streams on w1 are abandoned as soon as the pool marks it down and continued elsewhere.

![E5]({f})

{t}
""")
