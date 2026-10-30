"""E7 — traffic spike with the autoscaler on.

Locally the autoscaler runs with SCALE_FILE (it writes the desired replica
count to a file) and the harness plays the role of the Deployment
controller: it adds workers, or drains one through its sidecar before
stopping it. On Kubernetes the same binary scales the worker Deployment.
"""
import os
import subprocess
import threading
import time

import pandas as pd

from harness import BIN, EXE, Cluster, Monitor, _procs, log
from plot import rdir, COLORS, INK2, SERIES, load, md_table, plt, save, summarize, timeline, write_section

ENV = {
    "AUTOSCALER_TARGET_QUEUE_PER_WORKER": "2", "AUTOSCALER_TARGET_TTFT_P95_MS": "1500",
    "AUTOSCALER_MIN_REPLICAS": "2", "AUTOSCALER_MAX_REPLICAS": "8",
    "AUTOSCALER_UP_WINDOW": "10s", "AUTOSCALER_UP_COOLDOWN": "15s", "AUTOSCALER_DOWN_WINDOW": "60s",
    "AUTOSCALER_INTERVAL": "2s", "AUTOSCALER_SCRAPE_WINDOW": "20s",
}


def run(quick: bool = False) -> None:
    with Cluster("e7", workers=2, policy="relay") as c, Monitor(c, interval=0.5) as m:
        scale = c.dir / "replicas"
        (c.dir / "replicas.current").write_text("2\n")
        scale.write_text("2\n")
        env = dict(os.environ, ROUTER_URLS=",".join(c.router_urls), SCALE_FILE=str(scale),
                   METRICS_ADDR="127.0.0.1:27900", **ENV)
        asp = subprocess.Popen([str(BIN / f"autoscaler{EXE}")], env=env, stdout=open(c.logs / "autoscaler.log", "ab"),
                               stderr=subprocess.STDOUT)
        _procs.append(asp)
        stop = threading.Event()
        actions = []

        def controller():
            # Reconcile actual workers to the desired count, one change at a time.
            t0 = time.time()
            while not stop.is_set():
                try:
                    want = int(scale.read_text().strip())
                except Exception:
                    want = len(c.live_workers())
                live = c.live_workers()
                if want > len(live):
                    w = c.add_worker()
                    actions.append({"t": time.time() - t0, "action": f"add {w.id}", "workers": len(c.live_workers())})
                    log(f"E7 scale up -> {len(c.live_workers())}")
                elif want < len(live):
                    victim = max(live, key=lambda w: w.idx)
                    actions.append({"t": time.time() - t0, "action": f"drain {victim.id}", "workers": len(live)})
                    log(f"E7 draining {victim.id}")
                    c.drain_and_remove_worker(victim.idx)
                    actions.append({"t": time.time() - t0, "action": f"removed {victim.id}", "workers": len(c.live_workers())})
                (c.dir / "replicas.current").write_text(f"{len(c.live_workers())}\n")
                stop.wait(0.5)

        th = threading.Thread(target=controller, daemon=True)
        th.start()
        dur = "200s" if quick else "420s"
        c.loadgen("spike", "run.jsonl", duration=dur)
        stop.set()
        th.join()
        asp.kill()
        m.save(c.dir / "monitor.csv")
        pd.DataFrame(actions).to_csv(c.dir / "actions.csv", index=False)


SCHEDULE = [(0, 2), (60, 10), (180, 2)]  # mirrors experiments/workloads/spike.yaml


def analyze() -> None:
    p = rdir("e7") / "run.jsonl"
    if not p.exists():
        return
    df = load(p)
    mon = pd.read_csv(rdir("e7") / "monitor.csv")
    mon = mon[mon.router == 0]
    ap = rdir("e7") / "actions.csv"
    acts = pd.read_csv(ap) if ap.stat().st_size > 2 else pd.DataFrame(columns=["t", "action", "workers"])
    tl = timeline(df, 5.0)
    s = summarize(df)

    fig, axes = plt.subplots(3, 1, figsize=(11, 8), sharex=True)
    end = df.t_start.max()
    xs = [0] + [t for t, _ in SCHEDULE[1:]] + [end]
    ys = [r for _, r in SCHEDULE] + [SCHEDULE[-1][1]]
    axes[0].step(xs, ys, where="post", color=INK2, linewidth=1.5, linestyle="--", label="offered load (req/s)")
    axes[0].step(mon.t, mon.workers, where="post", color=SERIES[0], label="workers")
    axes[0].set(ylabel="count", title="Offered load and worker count")
    axes[0].legend(loc="upper right", fontsize=8)
    axes[1].plot(mon.t, mon.queue + mon.worker_queue, color=SERIES[0])
    axes[1].set(ylabel="queued requests", title="Queue depth (router + workers)")
    axes[2].plot(tl.b, tl.p50, color=COLORS["relay"], label="TTFT p50")
    axes[2].plot(tl.b, tl.p99, color=COLORS["relay"], linestyle="--", linewidth=1.5, label="TTFT p99")
    axes[2].axhline(1500, color=INK2, linewidth=1, linestyle=":")
    axes[2].annotate("TTFT p95 target", (0, 1500), fontsize=8, color=INK2, xytext=(2, 3), textcoords="offset points")
    axes[2].set(yscale="log", xlabel="time (s)", ylabel="TTFT (ms, log scale)", title="Client latency")
    axes[2].legend(loc="upper right", fontsize=8)
    for _, a in acts.iterrows():
        if str(a.action).startswith(("add", "drain")):
            axes[0].axvline(a.t, color="#e4e3df", linewidth=1, zorder=0)
    f = save(fig, "e7_autoscale")

    ups = acts[acts.action.str.startswith("add")] if len(acts) else acts
    drains = acts[acts.action.str.startswith("drain")] if len(acts) else acts
    spike = df[(df.t_start >= 60) & (df.t_start < 180) & df.ok]
    row = {**s, "max_workers": int(mon.workers.max()), "ups": len(ups), "drains": len(drains),
           "first_up": float(ups.t.min() - 60) if len(ups) else float("nan"),
           "p99_spike_first30": float(spike[spike.t_start < 90].ttft_ms.quantile(0.99)),
           "p99_spike_last60": float(spike[spike.t_start >= 120].ttft_ms.quantile(0.99)),
           "errors_during_scaledown": int((~df[df.t_start >= 180].ok).sum())}
    t = md_table([row], [("requests", "requests", "d"), ("errors", "client-visible errors", "d"),
                         ("max_workers", "peak workers", "d"), ("ups", "scale-up steps", "d"),
                         ("first_up", "first scale-up after spike (s)", ".0f"),
                         ("p99_spike_first30", "TTFT p99 first 30 s of spike (ms)", ".0f"),
                         ("p99_spike_last60", "TTFT p99 last 60 s of spike (ms)", ".0f"),
                         ("drains", "drained scale-downs", "d"), ("errors_during_scaledown", "errors during scale-down", "d")])
    write_section("e7", f"""
## E7 — Traffic spike with the autoscaler

Offered load steps 2 → 10 → 2 req/s (`spike` workload, 420 s). The autoscaler (targets: 2 queued requests per
worker, TTFT p95 1500 ms; 10 s sustained window to scale up, 60 s to scale down, min 2 / max 8) reads the routers'
metrics; scale-down drains a worker through its sidecar before stopping it.

A first attempt used the production TTFT target (800 ms) and a 30 s down window. It scaled up 41 s *before* the
spike, because on these workers a cold-cache prefill of a 1500-token system prompt alone takes ~950 ms. It also
started draining workers while the spike was still running, because with 8 workers both signals dropped below half
the target. It still had 0 client errors (raw data in `results/e7-first-attempt/`). Lesson: the TTFT target must
sit above the uncached-prefill cost of the workload, and the down window must outlast a burst.

![E7]({f})

{t}
""")
