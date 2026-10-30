"""Run one experiment: `python experiments/run.py e1 [--quick] [--analyze-only]`.

Each scenario module in experiments/scenarios/ exposes run(quick) (produces
raw results under results/<exp>/) and analyze() (turns them into figures in
docs/figures/ and a section in docs/results/); report.py then rebuilds
docs/report.md.
"""
import argparse
import importlib
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402
import report  # noqa: E402

NAMES = {
    "e1": "e1_policies", "e2": "e2_load_sweep", "e3": "e3_hot_prefix", "e4": "e4_worker_kill",
    "e1cpu": "e1_llamacpp", "e5": "e5_partition", "e6": "e6_router_kill", "e7": "e7_autoscale", "e8": "e8_param_sweep",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("name", help="e1..e8 or all")
    ap.add_argument("--quick", action="store_true", help="short runs (smoke test)")
    ap.add_argument("--analyze-only", action="store_true", help="re-plot existing results")
    a = ap.parse_args()
    quick = a.quick or os.environ.get("QUICK") == "1"
    names = list(NAMES) if a.name == "all" else [a.name.lower()]
    if not a.analyze_only:
        harness.build()
    for n in names:
        mod = importlib.import_module(f"scenarios.{NAMES[n]}")
        if not a.analyze_only:
            harness.log(f"running {n} ({'quick' if quick else 'full'})")
            mod.run(quick=quick)
        harness.log(f"analyzing {n}")
        mod.analyze()
    report.build()


if __name__ == "__main__":
    main()
