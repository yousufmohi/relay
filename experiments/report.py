"""Builds docs/report.md from docs/report.template.md.

The template is hand-written prose. Markers of the form
`<!-- include e1 -->` are replaced with the generated section for that
experiment (docs/results/e1.md), and `<!-- headline -->` with the headline
comparison table computed from the raw results.
"""
from __future__ import annotations

import re

from plot import POLICIES, VARIANTS, ROOT, SECTIONS, load, load_many, md_table, rdir, summarize

TEMPLATE = ROOT / "docs" / "report.template.md"
OUT = ROOT / "docs" / "report.md"
SHARED = ["shared-system-prompt", "multi-turn-chat", "rag-shared-docs"]


def headline() -> str:
    """Relay vs baselines: TTFT p99 and hit rate averaged over the shared-prefix
    workloads (E1), goodput (E2), hot-prefix p99 (E3/E1)."""
    rows = []
    e2 = {}
    try:
        from scenarios.e2_load_sweep import RATES, SLO_MS, WARMUP as W2

        for pol in POLICIES:
            best, okr = 0.0, 0
            for r in RATES:
                p = rdir("e2") / f"{pol}__{r}.jsonl"
                if p.exists():
                    s = summarize(load(p), warmup=W2, slo_ms=SLO_MS)
                    best = max(best, s["goodput"])
                    if s["ttft_p99"] <= SLO_MS and s["error_rate"] < 0.01:
                        okr = max(okr, r)
            e2[pol] = (best, okr)
    except Exception:
        pass
    for pol in POLICIES + list(VARIANTS):
        r = {"policy": f"**{pol}**" if pol.startswith("relay") else pol}
        p99s, hits = [], []
        for wl in SHARED:
            fs = sorted(rdir("e1").glob(f"{wl}__{pol}__s*.jsonl"))
            if fs:
                s = summarize(load_many(fs), warmup=10)
                p99s.append(s["ttft_p99"])
                hits.append(s["hit_rate"])
        if p99s:
            r["p99_shared"] = sum(p99s) / len(p99s)
            r["hit_shared"] = sum(hits) / len(hits)
        for wl, key in (("unique", "p99_unique"), ("hot-prefix", "p99_hot")):
            fs = sorted(rdir("e1").glob(f"{wl}__{pol}__s*.jsonl"))
            if fs:
                r[key] = summarize(load_many(fs), warmup=10)["ttft_p99"]
        if pol in e2:
            r["goodput"], r["max_rate"] = e2[pol]
        rows.append(r)
    return md_table(rows, [
        ("policy", "policy", ""),
        ("p99_shared", "TTFT p99, shared-prefix workloads (ms, mean of 3)", ".0f"),
        ("hit_shared", "cache hit rate, shared-prefix workloads", ".3f"),
        ("p99_unique", "TTFT p99, unique (ms)", ".0f"),
        ("p99_hot", "TTFT p99, hot-prefix (ms)", ".0f"),
        ("goodput", "peak goodput @ p99≤1s (req/s)", ".2f"),
        ("max_rate", "max load meeting p99≤1s (req/s)", "d"),
    ])


def build() -> None:
    if not TEMPLATE.exists():
        return
    text = TEMPLATE.read_text(encoding="utf-8")

    def include(m: re.Match) -> str:
        name = m.group(1)
        p = SECTIONS / f"{name}.md"
        if p.exists():
            return p.read_text(encoding="utf-8")
        if name.endswith(("-gpu", "-k8s")):
            return ""  # optional variants; the template explains how to produce them
        return f"_{name.upper()} has not been run yet (`make exp NAME={name}`)._\n"

    text = re.sub(r"<!-- include ([\w-]+) -->", include, text)
    text = text.replace("<!-- headline -->", headline())
    OUT.write_text(text, encoding="utf-8")
