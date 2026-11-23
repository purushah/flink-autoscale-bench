#!/usr/bin/env python3
"""E4 (restarts 60 s longer): the contrasts with all ten runs and with the six runs made after the
queue resumed (repetitions 3 to 5), as the plan entry of 2026-11-23 00:55 PST asks.

    python3 bench/paper.py e4          # first: writes results-e4/runs.csv
    python3 bench/e4_blocks.py

Per block: the pooled differences of paper.pooled() (share of the scenario's mean, floors as in the
main campaign) for PaC - Builtin, DM - PaC and LLM - PaC, the paired differences per scenario in the
metric's own unit, and the rescalings per job. No test beyond the sign-flip p that paper.pooled() gives.
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import paper  # noqa: E402

JOB = {"builtin+max70+rd60+rt70": "Builtin", "rule+rd60+rt70": "PaC", "jev+rd60+rt70": "DM", "llm+rd60+rt70": "LLM"}
NAME = {v: k for k, v in JOB.items()}
CONTRASTS = (("PaC", "Builtin"), ("DM", "PaC"), ("LLM", "PaC"))
COLS = (("slo_violation_s", "seconds over the limit"), ("slots_mean", "slots"))


def main():
    df = pd.read_csv("results-e4/runs.csv")
    for label, sub in (("all ten runs", df), ("the six runs of repetitions 3 to 5", df[df.rep >= 3])):
        runs = sub[["scenario", "rep"]].drop_duplicates()
        print(f"=== {label}: {len(runs)} runs")
        for col, what in COLS:
            for a, b in CONTRASTS:
                p = paper.pooled(sub, "default", NAME[a], NAME[b], col)
                print(f"  pooled {what:22s} {a:3s} - {b:7s}: {100 * p['mean']:+.1f}% [{100 * p['lo']:+.1f}, {100 * p['hi']:+.1f}] "
                      f"n={p['n']} sign-flip p={p['p']:.4f}")
        for sc in sorted(set(sub.scenario)):
            d = sub[sub.scenario == sc]
            piv = {col: d.pivot(index="rep", columns="job", values=col) for col in ("slo_violation_s", "slots_mean", "scaling_actions")}
            means = "  ".join(f"{JOB[j]} {piv['slo_violation_s'][j].mean():.0f} s / {piv['slots_mean'][j].mean():.2f} slots / "
                              f"{piv['scaling_actions'][j].mean():.1f} rescalings" for j in JOB)
            print(f"  {sc} ({len(piv['slo_violation_s'])} runs): {means}")
            for col, what in COLS:
                for a, b in CONTRASTS:
                    x = (piv[col][NAME[a]] - piv[col][NAME[b]]).dropna().to_numpy(float)
                    m, lo, hi = paper.boot_ci(x) if len(x) > 1 else (x.mean(), float("nan"), float("nan"))
                    print(f"     {what:22s} {a:3s} - {b:7s}: {m:+.2f} [{lo:+.2f}, {hi:+.2f}]  lower in {(x < 0).sum()} of {len(x)}; "
                          f"by run {', '.join(f'{v:+.1f}' for v in x)}")


if __name__ == "__main__":
    main()
