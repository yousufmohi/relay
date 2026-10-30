"""Shared analysis + plotting helpers for the experiments.

Figures go to docs/figures/<exp>_*.png; each experiment also writes a
markdown section (tables + figure links) to docs/results/<exp>.md, which
report.py stitches into docs/report.md.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
FIG = ROOT / "docs" / "figures"
SECTIONS = ROOT / "docs" / "results"

# Which deployment the results came from: "" = local simulated cluster,
# "k8s" = the k3s cluster (RELAY_TARGET=k8s), "gpu" = vLLM on GPUs
# (RELAY_VARIANT=gpu). Each variant keeps separate results/figures/sections.
VARIANT = os.environ.get("RELAY_VARIANT") or ("k8s" if os.environ.get("RELAY_TARGET") == "k8s" else "")
SUFFIX = f"-{VARIANT}" if VARIANT else ""


def rdir(exp: str) -> Path:
    """Raw results directory for an experiment in the current variant."""
    return ROOT / "results" / f"{exp}{SUFFIX}"

# Categorical colors in a FIXED order per entity (validated for CVD
# separation; slots 3-4 are below 3:1 contrast, so every figure ships with a
# table of the same numbers).
POLICIES = ["round_robin", "least_loaded", "consistent_hash", "relay"]
COLORS = {"round_robin": "#eb6834", "least_loaded": "#1baf7a", "consistent_hash": "#eda100", "relay": "#2a78d6",
          "relay_floor4": "#e87ba4"}
# Policy variants: name -> (POLICY, extra router env). relay_floor4 = relay with
# LOAD_BOUND_FLOOR=4 (the simulated workers' slot count), see E8.
VARIANTS = {"relay_floor4": ("relay", {"LOAD_BOUND_FLOOR": "4"})}
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.axisbelow": True,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "legend.frameon": False,
    "lines.linewidth": 2,
})


def load(path: Path) -> pd.DataFrame:
    rows = [json.loads(l) for l in Path(path).read_text(encoding="utf-8").splitlines() if l.strip()]
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ok"] = df["status"] == "ok"
    return df


def load_many(paths) -> pd.DataFrame:
    """Pool several runs (e.g. seeds) of the same configuration."""
    frames = [load(p) for p in paths]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def summarize(df: pd.DataFrame, warmup: float = 0.0, duration: float | None = None, slo_ms: float | None = None) -> dict:
    """Headline numbers for one run (requests started after warmup)."""
    if df.empty:
        df = pd.DataFrame(columns=["t_start", "ok", "ttft_ms", "prompt_tokens", "cached_tokens", "failovers"])
    d = df[df["t_start"] >= warmup]
    ok = d[d["ok"]]
    span = duration - warmup if duration else max(1e-9, d["t_start"].max() - warmup)
    prompt = ok["prompt_tokens"].sum()
    out = {
        "requests": len(d),
        "ok": len(ok),
        "errors": int((~d["ok"]).sum()),
        "error_rate": float((~d["ok"]).mean()) if len(d) else 0.0,
        "ttft_p50": float(ok["ttft_ms"].quantile(0.5)) if len(ok) else np.nan,
        "ttft_p99": float(ok["ttft_ms"].quantile(0.99)) if len(ok) else np.nan,
        "throughput": len(ok) / span,
        "hit_rate": float(ok["cached_tokens"].sum() / prompt) if prompt else 0.0,
        "failovers": int(d["failovers"].sum()),
    }
    if slo_ms is not None:
        out["goodput"] = float((ok["ttft_ms"] <= slo_ms).sum() / span)
    return out


def md_table(rows: list[dict], cols: list[tuple[str, str, str]]) -> str:
    """cols: (key, header, format)."""
    head = "| " + " | ".join(h for _, h, _ in cols) + " |"
    sep = "|" + "|".join("---:" if f else "---" for _, _, f in cols) + "|"
    body = []
    for r in rows:
        cells = []
        for k, _, f in cols:
            v = r.get(k, "")
            cells.append(format(v, f) if f and isinstance(v, (int, float)) and not pd.isna(v) else str(v))
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([head, sep, *body])


def save(fig, name: str) -> str:
    FIG.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIG / f"{name}{SUFFIX}.png", dpi=140)
    plt.close(fig)
    return f"figures/{name}{SUFFIX}.png"


def write_section(exp: str, text: str) -> None:
    SECTIONS.mkdir(parents=True, exist_ok=True)
    if VARIANT:  # label the heading so k8s/GPU sections are distinguishable from local ones
        head, _, rest = text.strip().partition("\n")
        text = f"{head} ({VARIANT})\n{rest}"
    (SECTIONS / f"{exp}{SUFFIX}.md").write_text(text.strip() + "\n", encoding="utf-8")


def grouped_bars(ax, groups: list[str], series: list[str], values: dict, colors: dict, ylabel: str, fmt: str = "{:.0f}"):
    """values[(group, series)] -> number. Thin bars with a 2px surface gap."""
    n = len(series)
    width = 0.8 / n
    x = np.arange(len(groups))
    for i, s in enumerate(series):
        ys = [values.get((g, s), np.nan) for g in groups]
        bars = ax.bar(x + (i - (n - 1) / 2) * width, ys, width, color=colors[s], label=s,
                      edgecolor=SURFACE, linewidth=1.5)
        if len(groups) * n <= 12:
            for b, y in zip(bars, ys):
                if not np.isnan(y):
                    ax.annotate(fmt.format(y), (b.get_x() + b.get_width() / 2, y), ha="center", va="bottom",
                                fontsize=7, color=INK2, xytext=(0, 2), textcoords="offset points")
    ax.set_xticks(x, groups)
    ax.set_ylabel(ylabel)
    ax.grid(axis="x", visible=False)


def timeline(df: pd.DataFrame, bucket: float = 2.0) -> pd.DataFrame:
    """Per-bucket p50/p99 TTFT, hit rate, errors over time."""
    d = df.copy()
    d["b"] = (d["t_start"] // bucket) * bucket
    g = d.groupby("b")
    out = pd.DataFrame({
        "p50": g.apply(lambda x: x.loc[x.ok, "ttft_ms"].quantile(0.5), include_groups=False),
        "p99": g.apply(lambda x: x.loc[x.ok, "ttft_ms"].quantile(0.99), include_groups=False),
        "hit": g.apply(lambda x: x.loc[x.ok, "cached_tokens"].sum() / max(1, x.loc[x.ok, "prompt_tokens"].sum()), include_groups=False),
        "errors": g.apply(lambda x: (~x.ok).sum(), include_groups=False),
        "n": g.size(),
    })
    return out.reset_index()
