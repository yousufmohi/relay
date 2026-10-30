"""E1 on real models: every policy on the docker-compose stack (router + 3
llama.cpp CPU workers running Qwen2.5-0.5B-Instruct Q4_K_M, 4 slots each).

CPU inference is slow, so the load is light (0.4 req/s, 16-token answers) and
only the shared-system-prompt workload is run. Workers are recreated before
every policy so each starts with empty caches.

    ROUTER_PORT=18080 python experiments/run.py e1cpu
"""
import os
import subprocess
import time
import urllib.request

from harness import BIN, EXE, ROOT, log
from plot import COLORS, POLICIES, grouped_bars, load, md_table, plt, rdir, save, summarize, write_section

PORT = os.environ.get("ROUTER_PORT", "18080")
URL = f"http://localhost:{PORT}"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "deploy" / "docker-compose.yml")]
RATE, DURATION, MAX_TOKENS, WARMUP = 0.4, "120s", 16, 15.0


def wait_ready(timeout: float = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(URL + "/readyz", timeout=2)
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError("compose stack not ready")


def run(quick: bool = False) -> None:
    out = rdir("e1cpu")
    out.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, ROUTER_PORT=PORT)
    for pol in POLICIES:
        log(f"E1-cpu {pol}: recreating workers (cold caches) and router")
        env["POLICY"] = pol
        subprocess.run([*COMPOSE, "up", "-d", "--force-recreate", "router"], env=env, check=True, capture_output=True)
        wait_ready()
        time.sleep(3)  # let sidecar streams connect
        subprocess.run([str(BIN / f"loadgen{EXE}"), "-targets", URL,
                        "-workload", str(ROOT / "experiments" / "workloads" / "shared-system-prompt.yaml"),
                        "-rate", str(RATE), "-duration", "40s" if quick else DURATION, "-max-tokens", str(MAX_TOKENS),
                        "-out", str(out / f"{pol}.jsonl")], check=True, cwd=ROOT)


def analyze() -> None:
    rows, vals = [], {}
    for pol in POLICIES:
        p = rdir("e1cpu") / f"{pol}.jsonl"
        if p.exists():
            s = summarize(load(p), warmup=WARMUP)
            rows.append({"policy": pol, **s})
            for k in ("ttft_p50", "ttft_p99", "hit_rate"):
                vals[(k, pol)] = s[k]
    if not rows:
        return
    pols = [r["policy"] for r in rows]
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))
    for ax, (key, label, fmt) in zip(axes, [("ttft_p50", "TTFT p50 (ms)", "{:.0f}"), ("ttft_p99", "TTFT p99 (ms)", "{:.0f}"),
                                            ("hit_rate", "prefix-cache hit rate", "{:.2f}")]):
        grouped_bars(ax, [""], pols, {("", p): vals[(key, p)] for p in pols}, COLORS, label, fmt)
        ax.set_title(label)
        ax.set_xticks([])
    axes[0].legend(fontsize=8, loc="upper left")
    f = save(fig, "e1_llamacpp")
    t = md_table(rows, [("policy", "policy", ""), ("requests", "requests", "d"), ("ttft_p50", "TTFT p50 ms", ".0f"),
                        ("ttft_p99", "TTFT p99 ms", ".0f"), ("hit_rate", "hit rate (llama.cpp cache_n)", ".3f"),
                        ("errors", "errors", "d")])
    write_section("e1cpu", f"""
## E1 on real models — llama.cpp CPU workers

The docker-compose stack: router + 3 llama.cpp workers (Qwen2.5-0.5B-Instruct Q4_K_M, 4 slots, 2 CPU threads each)
with sidecars, `shared-system-prompt` at {RATE} req/s for {DURATION} with {MAX_TOKENS}-token answers (CPU prefill
of a ~1.3k-token system prompt takes seconds, so the load is light). Hit rate comes from llama.cpp's own
`timings.cache_n`. One run per policy, so treat p99 (a handful of requests) as indicative only.

![E1 llama.cpp]({f})

{t}
""")
