"""E6 — kill one of two router replicas: dropped requests and cache-hit dip."""
import threading
import time

import pandas as pd

from harness import Cluster, log
from plot import rdir, COLORS, INK2, SERIES, load, md_table, plt, save, summarize, timeline, write_section

KILL_AT, RESTART_AT = 20.0, 35.0


def run(quick: bool = False) -> None:
    with Cluster("e6", workers=4, routers=2, policy="relay") as c:
        lg = c.loadgen("shared-system-prompt", "run.jsonl", rate=5, duration="60s", background=True,
                       extra=["-max-tokens", "100"])
        t0 = time.time()

        def chaos():
            time.sleep(KILL_AT)
            log("E6 kill router-1")
            c.kill_router(1)
            time.sleep(max(0, RESTART_AT - (time.time() - t0)))
            log("E6 restart router-1 (empty prefix index)")
            c.start_router(1)

        th = threading.Thread(target=chaos)
        th.start()
        lg.wait()
        th.join()


def analyze() -> None:
    p = rdir("e6") / "run.jsonl"
    if not p.exists():
        return
    df = load(p)
    s = summarize(df)
    tl = timeline(df, 3.0)
    r1 = df[df.target.str.endswith(":26001")]
    tl1 = timeline(r1, 3.0) if len(r1) else None

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6), sharex=True)
    a1.plot(tl.b, tl.hit, color=SERIES[0], label="all requests")
    if tl1 is not None:
        # Reindex to the full time range so the line breaks while router-1 is down.
        tl1 = tl1.set_index("b").reindex(tl.b).reset_index()
        a1.plot(tl1.b, tl1.hit, color=SERIES[1], label="requests served by router-1")
    a1.set(ylabel="prefix-cache hit rate", title="Cache hit rate over time", ylim=(0, 1))
    a1.legend(loc="lower left", fontsize=8)
    retried = df[df.conn_retries > 0]
    a2.bar(tl.b, tl.n, width=2.6, color=SERIES[0], label="requests", edgecolor="#fcfcfb")
    rt = retried.groupby((retried.t_start // 3) * 3).size()
    a2.bar(rt.index, rt.values, width=2.6, color=SERIES[1], label="needed a connection retry", edgecolor="#fcfcfb")
    a2.set(xlabel="time (s)", ylabel="requests per 3 s", title="Requests and client connection retries")
    a2.legend(loc="upper right", fontsize=8)
    for ax in (a1, a2):
        ax.axvline(KILL_AT, color=INK2, linestyle=":", linewidth=1)
        ax.axvline(RESTART_AT, color=INK2, linestyle=":", linewidth=1)
    a1.annotate("kill router-1", (KILL_AT, 0.05), fontsize=8, color=INK2, xytext=(3, 0), textcoords="offset points")
    a1.annotate("restart (empty index)", (RESTART_AT, 0.05), fontsize=8, color=INK2, xytext=(3, 0), textcoords="offset points")
    f = save(fig, "e6_router_kill")

    def hit(lo, hi):
        d = df[(df.t_start >= lo) & (df.t_start < hi) & df.ok]
        return d.cached_tokens.sum() / max(1, d.prompt_tokens.sum())

    row = {**s, "retried": len(retried), "hit_before": hit(10, KILL_AT), "hit_one": hit(KILL_AT, RESTART_AT),
           "hit_after5": hit(RESTART_AT, RESTART_AT + 5), "hit_end": hit(RESTART_AT + 10, 60)}
    t = md_table([row], [("requests", "requests", "d"), ("errors", "dropped (client-visible errors)", "d"),
                         ("retried", "requests that needed a connection retry", "d"),
                         ("hit_before", "hit rate before kill", ".3f"), ("hit_one", "one router", ".3f"),
                         ("hit_after5", "first 5 s after restart", ".3f"), ("hit_end", "last 15 s", ".3f")])
    write_section("e6", f"""
## E6 — Killing a router replica

Two router replicas, each building its own prefix index from its own decisions and its own sidecar streams (no shared
state). The load generator spreads requests over both and retries connection failures on the other replica. Router-1
is killed at 20 s and restarted at 35 s with an empty index. (Requests are bucketed by start time, so streams that
started on router-1 shortly before the kill show up as retries just before 20 s.)

![E6]({f})

{t}
""")
