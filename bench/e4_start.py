#!/usr/bin/env python3
"""E4 (restarts 60 s longer): the state of the jobs when the measured phase begins and when the load
changes, and the contrast PaC - Builtin split by that state. Written on 23 Nov 2026 after review
round 4 (paper/reviews/round4/R2_ml_methodology.md, W1) and checked against that review's numbers.

    python3 bench/paper.py e4          # first: writes results-e4/runs.csv
    python3 bench/e4_start.py [results dir, default results-e4]

Per run: the seconds over the limit of PaC and Builtin (the paper's definition, bench/analyze.py: p95
above the limit, or no output for more than 3 s) in the whole measured phase, before the load change
and from it on; Aggregate's size at the start of the measured phase and just before the load change.
Then the contrast as a share of the scenario's mean over both jobs and all runs, for all runs and
split by a state that uses no outcome: Builtin at the scenario's size just before the load change, or
above it. Also how many job-runs scaled up in the warm-up and how many runs begin with all four jobs
at the scenario's size.
"""
import glob
import json
import os
import sys
import warnings

warnings.filterwarnings("ignore")
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze  # noqa: E402

B, P = "builtin+max70+rd60+rt70", "rule+rd60+rt70"


def ci(x):
    x = np.asarray(x, float)
    half = stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / np.sqrt(len(x))
    return f"{100 * x.mean():+.1f}% [{100 * (x.mean() - half):+.1f}, {100 * (x.mean() + half):+.1f}], PaC longer in {(x > 0).sum()} of {len(x)}"


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "results-e4"
    rows, warm_ups, job_runs, equal = [], 0, 0, 0
    for d in sorted(glob.glob(os.path.join(root, "default", "*", "rep*"))):
        m = json.load(open(os.path.join(d, "meta.json")))
        sc, rep, ev, dur = d.split("/")[-2], int(d.rsplit("rep", 1)[1]), m["events"][0], m["duration"]
        latency = pd.read_csv(os.path.join(d, "latency.csv"))
        samples = pd.read_csv(os.path.join(d, "samples.csv"))
        at_size = True
        for job in m["jobs"]:
            s = samples[(samples.job == job) & samples.p_aggregate.notna()]
            warm = s[s.t < 0].p_aggregate.to_numpy()
            warm_ups += bool((np.diff(warm) > 0).any())
            job_runs += 1
            at_size &= int(s[s.t >= 0].p_aggregate.iloc[0]) == m["initial_parallelism"][job][0]
        equal += at_size
        row = dict(scenario=sc, rep=rep)
        for job, tag in ((P, "pac"), (B, "bi")):
            healthy = analyze.healthy_series(analyze.per_second(latency, job, dur).p95).loc[0:dur]
            over = healthy.isna() | (healthy > analyze.SLO_MS)
            row[tag + "_all"], row[tag + "_pre"], row[tag + "_post"] = int(over.sum()), int(over.loc[:ev - 1e-9].sum()), int(over.loc[ev:].sum())
            s = samples[(samples.job == job) & samples.p_aggregate.notna()]
            row[tag + "_t0"], row[tag + "_before"] = int(s[s.t >= 0].p_aggregate.iloc[0]), int(s[s.t < ev].p_aggregate.iloc[-1])
            row[tag + "_init"] = m["initial_parallelism"][job][0]
        rows.append(row)
    df = pd.DataFrame(rows)
    print("per run: seconds over the limit (whole phase, before the load change, from it on) and Aggregate's size (start of the measured phase, before the load change)")
    print(df[["scenario", "rep", "pac_all", "bi_all", "pac_pre", "bi_pre", "pac_post", "bi_post", "pac_t0", "pac_before", "bi_t0", "bi_before", "bi_init"]].to_string(index=False))
    path = os.path.join(root, "runs.csv")
    if os.path.exists(path):
        chk = pd.read_csv(path).pivot_table(index=["scenario", "rep"], columns="job", values="slo_violation_s")
        same = sum(chk.loc[(s, r)][P] == a and chk.loc[(s, r)][B] == b for s, r, a, b in zip(df.scenario, df.rep, df.pac_all, df.bi_all))
        print(f"seconds agree with runs.csv in {same} of {len(df)} runs")
    print(f"job-runs that scaled Aggregate up in the warm-up: {warm_ups} of {job_runs}; runs that begin with all jobs at the scenario's size: {equal} of {len(df)}")
    above = df.bi_before > df.bi_init
    print(f"Builtin above the scenario's size just before the load change: {int(above.sum())} of {len(df)} runs; "
          f"PaC smaller before the load change than at the start: {int((df.pac_before < df.pac_t0).sum())}")
    mean = {sc: (g.pac_all.mean() + g.bi_all.mean()) / 2 for sc, g in df.groupby("scenario")}
    for label, a, b in (("whole measured phase", "pac_all", "bi_all"), ("before the load change", "pac_pre", "bi_pre"),
                        ("from the load change on", "pac_post", "bi_post")):
        rel = (df[a] - df[b]) / df.scenario.map(mean)
        print(f"{label}: PaC {df[a].mean():.1f} s, Builtin {df[b].mean():.1f} s; PaC - Builtin as a share of the scenario's mean: all {ci(rel)}; "
              f"Builtin at the scenario's size (n={int((~above).sum())}) {ci(rel[~above])}; above it (n={int(above.sum())}) {ci(rel[above])}")


if __name__ == "__main__":
    main()
