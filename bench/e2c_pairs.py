#!/usr/bin/env python3
"""Identical jobs side by side: the statistic of the main campaign's planned contrasts applied to two
jobs that differ in nothing (plan entry of 2026-11-22 09:54 PST, campaign E2c).

    python3 bench/paper.py e2c                      # first: writes results-e2c/runs.csv
    python3 bench/e2c_pairs.py [--root results-e2c] [--arm aa-rule] [--tex paper/gen/numbers_e2c_pairs.tex]

Per run the job with tag b minus the job with tag a (and d minus c), divided by the mean of the two in
the scenario with the floor of the pooled contrast (30 s, half a slot), pooled over the scenarios: mean,
Student's t interval and the standard deviation per pair; per scenario the differences in the metric's
own unit and the largest absolute one. Next to it the same statistic for the main campaign's two model
contrasts restricted to the same scenarios (default fetch size), from results-v3/runs.csv.
Works on any campaign with jobs tagged +a to +d (try --root results-e2)."""
import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import paper  # noqa: E402

COLS = (("slo_violation_s", "seconds over the limit"), ("slots_mean", "slots"))


def relative(df, a, b, col, scenarios):
    """(scenario, rep, difference, difference / scale) for b minus a, as paper.pooled() scales it."""
    rows = []
    for sc in scenarios:
        d = df[df.scenario == sc].pivot(index="rep", columns="job", values=col)
        if a not in d or b not in d:
            continue
        scale = max(np.nanmean(np.r_[d[a].to_numpy(float), d[b].to_numpy(float)]), paper.POOL_FLOOR.get(col, 0.0))
        for rep, x in (d[b] - d[a]).dropna().items():
            rows.append((sc, rep, float(x), float(x) / scale if scale > 0 else np.nan))
    return pd.DataFrame(rows, columns=["scenario", "rep", "diff", "rel"])


def line(label, r):
    m, lo, hi = paper.boot_ci(r.rel.to_numpy(float))
    sd = r.rel.std(ddof=1)
    return (f"  {label}: {100 * m:+.1f}% [{100 * lo:+.1f}, {100 * hi:+.1f}], spread per pair {100 * sd:.0f}%, {len(r)} pairs", m, lo, hi, sd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(paper.ROOT, "results-e2c"))
    ap.add_argument("--arm", default="aa-rule")
    ap.add_argument("--tex", default=None)
    args = ap.parse_args()
    df = pd.read_csv(os.path.join(args.root, "runs.csv"))
    df = df[df.arm == args.arm]
    base = sorted({j.rsplit("+", 1)[0] for j in df.job if j.rsplit("+", 1)[-1] in "abcd"})[0]
    scenarios = [s for s in paper.ORDER if s in set(df.scenario)] + sorted(set(df.scenario) - set(paper.ORDER))
    numbers = paper.Numbers()
    print(f"{args.root}, arm {args.arm}: {df.groupby(['scenario', 'rep']).ngroups} runs, scenarios {', '.join(scenarios)}, jobs {base}+a to +d")
    for col, label in COLS:
        print(f"=== {label}")
        for a, b in (("a", "b"), ("c", "d")):
            r = relative(df, f"{base}+{a}", f"{base}+{b}", col, scenarios)
            if not len(r):
                continue
            text, m, lo, hi, sd = line(f"{b} minus {a}, pooled over the scenarios", r)
            print(text)
            key = ("e2c", "pair", a + b, "slo" if col == "slo_violation_s" else "slots")
            for field, v in (("mean", m), ("lo", lo), ("hi", hi)):
                numbers.add(paper.macro_name(*key, field), f"{100 * v:+.1f}".replace("+", "$+$").replace("-", "$-$"))
            numbers.add(paper.macro_name(*key, "spread"), f"{100 * sd:.0f}")
            numbers.add(paper.macro_name(*key, "n"), str(len(r)))
            for sc, part in r.groupby("scenario", sort=False):
                dm, dlo, dhi = paper.boot_ci(part["diff"].to_numpy(float))
                print(f"     {sc}: {dm:+.1f} [{dlo:+.1f}, {dhi:+.1f}] in the metric's unit, largest absolute difference {part['diff'].abs().max():.1f}, "
                      f"spread per pair {100 * part.rel.std(ddof=1):.0f}%, {len(part)} runs")
                skey = key + (sc,)
                numbers.add(paper.macro_name(*skey, "spread"), f"{100 * part.rel.std(ddof=1):.0f}")
                numbers.add(paper.macro_name(*skey, "largest"), f"{part['diff'].abs().max():.0f}")
    main_csv = os.path.join(paper.ROOT, "results-v3", "runs.csv")
    if os.path.exists(main_csv):
        mdf = pd.read_csv(main_csv)
        mdf = mdf[mdf.arm == "default"]
        print("=== for comparison: the main campaign's model contrasts on the same scenarios (default fetch size)")
        for model, name in (("jev", "DM"), ("llm", "LLM")):
            r = relative(mdf, "rule", model, "slo_violation_s", [s for s in scenarios if s in set(mdf.scenario)])
            if len(r):
                text, m, lo, hi, sd = line(f"{name} minus PaC, seconds over the limit", r)
                print(text)
                key = ("e2c", "main", name, "slo")
                for field, v in (("mean", m), ("lo", lo), ("hi", hi)):
                    numbers.add(paper.macro_name(*key, field), f"{100 * v:+.1f}".replace("+", "$+$").replace("-", "$-$"))
                numbers.add(paper.macro_name(*key, "spread"), f"{100 * sd:.0f}")
    if args.tex:
        numbers.write(args.tex)
        print(f"wrote {args.tex}")


if __name__ == "__main__":
    main()
