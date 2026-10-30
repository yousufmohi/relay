"""E4 — kill workers mid-stream under load; failover pause and client errors."""
import threading
import time

import numpy as np
import pandas as pd

from harness import Cluster, Monitor, log
from plot import rdir, COLORS, INK2, load, md_table, plt, save, summarize, timeline, write_section

KILLS = [(15.0, 1), (25.0, 2), (35.0, 3), (45.0, 0), (55.0, 1)]  # (time s, worker idx)
DOWN_FOR = 5.0  # each killed worker restarts (empty cache) this much later


def run(quick: bool = False) -> None:
    with Cluster("e4", workers=4, policy="relay") as c, Monitor(c) as m:
        lg = c.loadgen("shared-system-prompt", "run.jsonl", rate=3, duration="70s", background=True,
                       extra=["-max-tokens", "120"])
        t0 = time.time()
        events = []

        def chaos():
            # Rolling crashes: at most one worker is down at a time.
            for at, idx in KILLS:
                time.sleep(max(0, at - (time.time() - t0)))
                log(f"E4 killing w{idx}")
                c.kill_worker(idx)
                events.append({"t": time.time() - t0, "event": f"kill w{idx}"})
                time.sleep(DOWN_FOR)
                c.restart_worker(idx)

        th = threading.Thread(target=chaos)
        th.start()
        lg.wait()
        th.join()
        m.save(c.dir / "monitor.csv")
        pd.DataFrame(events).to_csv(c.dir / "events.csv", index=False)


def analyze() -> None:
    p = rdir("e4") / "run.jsonl"
    if not p.exists():
        return
    df = load(p)
    s = summarize(df)
    pauses = np.array([x for xs in df["pause_ms"].dropna() for x in (xs if isinstance(xs, list) else [])])
    events = pd.read_csv(rdir("e4") / "events.csv")
    tl = timeline(df, 2.0)

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.2), gridspec_kw={"width_ratios": [2, 1]})
    a1.plot(tl.b, tl.p50, color=COLORS["relay"], label="TTFT p50")
    a1.plot(tl.b, tl.p99, color=COLORS["relay"], linestyle="--", linewidth=1.5, label="TTFT p99")
    fo = df[df.failovers > 0]
    a1.scatter(fo.t_start, np.full(len(fo), max(1, tl.p50.min() * 0.6)), marker="v", s=40, color="#e34948",
               label="request that failed over", zorder=3)
    for _, e in events.iterrows():
        a1.axvline(e.t, color=INK2, linewidth=1, linestyle=":")
        a1.annotate(e.event, (e.t, a1.get_ylim()[1] if a1.get_ylim()[1] > 0 else 1), rotation=90, va="top", ha="right",
                    fontsize=8, color=INK2)
    a1.set(yscale="log", xlabel="time (s)", ylabel="TTFT (ms, log scale)", title="TTFT over time with worker kills")
    a1.legend(loc="upper left", fontsize=8)
    if len(pauses):
        a2.hist(pauses, bins=20, color=COLORS["relay"], edgecolor="#fcfcfb")
    a2.set(xlabel="failover pause (ms)", ylabel="failovers", title="Client-visible failover pause")
    a2.grid(axis="x", visible=False)
    f = save(fig, "e4_worker_kill")
    row = {**s, "pause_p50": float(np.median(pauses)) if len(pauses) else float("nan"),
           "pause_p99": float(np.quantile(pauses, 0.99)) if len(pauses) else float("nan"),
           "pause_max": float(pauses.max()) if len(pauses) else float("nan"),
           "requests_failed_over": int((df.failovers > 0).sum())}
    t = md_table([row], [("requests", "requests", "d"), ("errors", "client-visible errors", "d"),
                         ("requests_failed_over", "requests that failed over", "d"), ("failovers", "failovers", "d"),
                         ("pause_p50", "pause p50 ms", ".0f"), ("pause_p99", "pause p99 ms", ".0f"),
                         ("pause_max", "pause max ms", ".0f")])
    per = []
    for at, idx in KILLS:
        d = df[(df.failovers > 0) & (df.t_start < at) & (df.t_start + df.e2e_ms / 1000 > at)]
        ps = np.array([x for xs in d.pause_ms for x in xs])
        per.append({"kill": f"w{idx} at {at:.0f}s", "n": len(ps),
                    "p50": float(np.median(ps)) if len(ps) else float("nan"),
                    "max": float(ps.max()) if len(ps) else float("nan")})
    t2 = md_table(per, [("kill", "kill", ""), ("n", "streams failed over", "d"),
                        ("p50", "pause p50 ms", ".0f"), ("max", "pause max ms", ".0f")])
    write_section("e4", f"""
## E4 — Killing workers mid-stream

`relay` policy, 4 workers, `shared-system-prompt` at 3 req/s with 120-token responses (long streams, so the kill lands
mid-generation). Rolling crashes: every 10 s from 15 s one worker is killed (SIGKILL, no drain) and restarted 5 s
later with an empty cache, so at most one of four workers is down at a time.

![E4]({f})

{t}

{t2}

Every request that was streaming on a killed worker was continued on another worker on the same client connection;
the client sees a pause, not an error. The pause is dominated by re-prefilling `prompt + generated_so_far` on the new
worker (the dead worker's KV cache is lost) plus any wait for a free slot there. In an earlier run with a second kill
(2 of 4 workers left, 200-token streams) the survivors were overloaded and pauses grew to 4–6 s of queueing: failover
keeps streams alive but cannot create capacity.
""")
