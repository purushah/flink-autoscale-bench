#!/usr/bin/env python3
"""Table of one arm of campaign lit: per load shape and controller, the paper's four main columns.

No charts (light on the host while another run is measuring). Refuses to read an arm that is not
complete, as the rule for this campaign says.

Usage: python3 bench/lit_arm.py <arm dir> [--extra <run dir> ...] [--csv <out.csv>] [--allow-partial]
   python3 bench/lit_arm.py results-lit/default --extra results-lit-first/default/onoff/rep1 --csv paper/gen/lit_default.csv
   python3 bench/lit_arm.py results-lit/accurate --csv paper/gen/lit_accurate.csv
"""
import argparse
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "bench"))
import analyze  # noqa: E402
import pandas as pd  # noqa: E402

SHAPES = ["replay", "steady", "onoff", "walk1", "cycle", "levels1"]
COLS = [("slo_violation_s", "Seconds over the 2 s limit", "{:.0f}"),
        ("latency_p95_ms", "p95 latency (s)", None),
        ("slots_mean", "Mean slots", "{:.2f}"),
        ("scaling_actions", "Scaling actions", "{:.0f}"),
        ("lag_max_records", "Largest backlog (thousand records)", None)]


def rows_of(run_dir, label):
    run = analyze.load_run(run_dir)
    meta = run[0]
    analyze.JOBS = tuple(meta["jobs"])
    out = []
    for job in meta["jobs"]:
        m = analyze.metrics(*run, job)
        out.append({"run": label, "scenario": meta["scenario"], "rep": meta["rep"], "job": job,
                    "duration_s": meta["duration"], **m})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("arm")
    ap.add_argument("--extra", nargs="*", default=[])
    ap.add_argument("--csv")
    ap.add_argument("--allow-partial", action="store_true")
    args = ap.parse_args()

    found = sorted(glob.glob(os.path.join(args.arm, "*", "rep*", "meta.json")))
    have = {os.path.basename(os.path.dirname(os.path.dirname(p))) for p in found}
    missing = [s for s in SHAPES if s not in have]
    if missing and not args.allow_partial and have & set(SHAPES):
        raise SystemExit(f"arm not complete, missing: {', '.join(missing)}; not read")

    records = []
    for p in found:
        records += rows_of(os.path.dirname(p), "arm")
    for d in args.extra:
        records += rows_of(d, "first:" + os.path.relpath(d, ROOT))
    df = pd.DataFrame(records)
    if args.csv:
        df.to_csv(args.csv, index=False)

    jobs = list(dict.fromkeys(df.job))
    order = [s for s in SHAPES if s in set(df.scenario)] + sorted(set(df.scenario) - set(SHAPES))
    for col, name, f in COLS:
        print(f"\n{name}")
        print("| shape | min | rep | " + " | ".join(jobs) + " |")
        print("|---|---|---|" + "---|" * len(jobs))
        for run_label in dict.fromkeys(df.run):
            for sc in order:
                d = df[(df.run == run_label) & (df.scenario == sc)]
                for rep in sorted(set(d.rep)):
                    cells = []
                    for j in jobs:
                        x = d[(d.rep == rep) & (d.job == j)]
                        if not len(x):
                            cells.append("-")
                            continue
                        v = float(x[col].iloc[0])
                        if col == "latency_p95_ms":
                            s = f"{v / 1000:.2f}"
                        elif col == "lag_max_records":
                            s = f"{v / 1000:.1f}"
                        else:
                            s = f.format(v)
                        if col == "slo_violation_s" and bool(x.slo_censored.iloc[0]):
                            s += " (still over at the end)"
                        cells.append(s)
                    tag = sc if run_label == "arm" else f"{sc} (first run, set aside)"
                    print(f"| {tag} | {d.duration_s.iloc[0] / 60:.0f} | {rep} | " + " | ".join(cells) + " |")

    print("\nModel choices against the rule's choice on the same inputs (agree / asked), and fallbacks")
    for run_label in dict.fromkeys(df.run):
        for sc in order:
            for rep_no in sorted(set(df[(df.run == run_label) & (df.scenario == sc)].rep)):
                d = df[(df.run == run_label) & (df.scenario == sc) & (df.rep == rep_no)]
                parts = []
                for j in jobs:
                    x = d[d.job == j]
                    if len(x) and "decisions_asked" in x and pd.notna(x.decisions_asked.iloc[0]):
                        fb = x.model_fallbacks.iloc[0] if "model_fallbacks" in x and pd.notna(x.model_fallbacks.iloc[0]) else 0
                        parts.append(f"{j} {int(x.decisions_agree.sum())}/{int(x.decisions_asked.sum())}"
                                     f" (above {int(x.decisions_above_rule.sum())}, below {int(x.decisions_below_rule.sum())}, fallbacks {int(fb)})")
                if parts:
                    print(f"  {sc} rep {rep_no}{'' if run_label == 'arm' else ' (first run)'}: " + "; ".join(parts))
    print("\nnot-RUNNING samples per job:",
          {(r.run, r.scenario, r.rep, r.job): int(r.not_running_samples) for r in df.itertuples() if r.not_running_samples})


if __name__ == "__main__":
    main()
