#!/usr/bin/env python3
"""Statistics, tables and figures for the paper, computed from the closed-loop campaigns.

  python3 bench/paper.py main                  # reads results-v3/, writes paper/gen/ and paper/figs/
  python3 bench/paper.py e8 | e3 | e4 | e2     # the other campaigns of bench/campaign.sh
  python3 bench/paper.py main --root some/dir --out some/out

Input layout: <root>/<arm>/<scenario>/rep<k>/. The main campaign has the arms "default" and
"accurate" (default and fresh backlog signal); the others have one arm or one per configuration.

Outputs:
  <root>/runs.csv            one row per arm, scenario, repetition and job
  <out>/gen/numbers.tex      every number quoted in the text, as a LaTeX macro
  <out>/gen/tab_*.tex        tables
  <out>/figs/*.pdf           figures
  <out>/gen/summary.txt      plain-text digest of the comparisons
"""
import argparse
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from statistics import NormalDist

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze  # noqa: E402
import scenarios  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORDER = ["ramp", "spike", "scale_down", "skew", "capacity", "ramp_long", "spike_long"]
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, VIOLET = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#6250d6"
# One entry per campaign: where its runs are, its arms, and its jobs in display order with the name,
# colour and marker used in the paper. Colours follow the entity: the built-in rule is blue, the
# policy in code yellow, the decision model orange, the LLM aqua.
# The primary contrasts of the analysis plan, by display name: the policy in code against the built-in
# rule, and each model against the policy in code. Holm's correction covers these on the primary
# metrics; every other comparison is reported with an interval and without a p-value.
MODEL_CONTRASTS = [("PaC", "Builtin"), ("DM", "PaC"), ("LLM", "PaC")]
EXPERIMENTS = {
    "main": {
        "root": "results-v3", "arms": {"default": "default backlog signal", "accurate": "fresh backlog signal"},
        "primary": MODEL_CONTRASTS,
        "jobs": [("builtin+max70", "Builtin", BLUE, "o"), ("rule", "PaC", YELLOW, "D"), ("jev", "DM", ORANGE, "s"),
                 ("llm", "LLM", AQUA, "^")],
    },
    "pilot": {
        "root": "results-pilot-v2", "arms": {"default": "default backlog signal"},
        "jobs": [("builtin", "Builtin", BLUE, "o"), ("rule", "PaC", YELLOW, "D"), ("jev", "DM", ORANGE, "s"),
                 ("llm", "LLM", AQUA, "^")],
    },
    "e8": {
        "root": "results-e8", "arms": {"default": "built-in rule"},
        # The default signal comes first: the other jobs are compared with it. The plan names one
        # primary comparison, on the seconds over the limit and on the number of scale-ups.
        "primary": [("Broker-side signal", "Default signal")], "primary_cols": ["slo_violation_s", "scale_ups"],
        "jobs": [("builtin+max70", "Default signal", BLUE, "o"), ("builtin+max70+lagfix", "Broker-side signal", AQUA, "D"),
                 ("builtin+max70+f4096", "4 KiB fetch", YELLOW, "s"), ("builtin", "Stock band", VIOLET, "v")],
    },
    "e8b": {
        "root": "results-e8b", "arms": {"default": "sustained overload"}, "pool": False,
        # Band by signal. The plan names the effect of the signal within each band as primary, on the
        # seconds over the limit and on the number of scale-ups.
        "jobs": [("builtin", "Stock band", VIOLET, "v"), ("builtin+lagfix", "Stock band, broker signal", MAGENTA, "s"),
                 ("builtin+max70", "Matched band", BLUE, "o"), ("builtin+max70+lagfix", "Matched band, broker signal", AQUA, "D")],
        "pairs": [("Stock band, broker signal", "Stock band"), ("Matched band, broker signal", "Matched band")],
        "primary": [("Stock band, broker signal", "Stock band"), ("Matched band, broker signal", "Matched band")],
        "primary_cols": ["slo_violation_s", "scale_ups"],
    },
    "e10": {
        # The four controllers in the sustained overload. In the second arm every job takes backlog and
        # arrival rate from the broker; its jobs carry that option in their names and are shown under
        # the names of the first arm.
        "root": "results-e10", "arms": {"default": "default signal", "broker": "broker-side signal"},
        "alias": {"builtin+max70+lagfix": "builtin+max70", "rule+lagfix": "rule", "jev+lagfix": "jev", "llm+lagfix": "llm"},
        "jobs": [("builtin+max70", "Builtin", BLUE, "o"), ("rule", "PaC", YELLOW, "D"), ("jev", "DM", ORANGE, "s"),
                 ("llm", "LLM", AQUA, "^")],
        "primary": MODEL_CONTRASTS,
    },
    "e8c": {
        "root": "results-e8c", "arms": {"long": "sustained overload, 5-minute windows"}, "pool": False,
        # Band by signal. The plan names the effect of the signal within each band as primary, on the
        # seconds over the limit and on the number of scale-ups.
        "jobs": [("builtin", "Stock band", VIOLET, "v"), ("builtin+lagfix", "Stock band, broker signal", MAGENTA, "s"),
                 ("builtin+max70", "Matched band", BLUE, "o"), ("builtin+max70+lagfix", "Matched band, broker signal", AQUA, "D")],
        "pairs": [("Stock band, broker signal", "Stock band"), ("Matched band, broker signal", "Matched band")],
        "primary": [("Stock band, broker signal", "Stock band"), ("Matched band, broker signal", "Matched band")],
        "primary_cols": ["slo_violation_s", "scale_ups"],
    },
    "e8d": {
        "root": "results-e8d", "arms": {"default": "moderate sustained overload"}, "pool": False,
        # Band by signal. The plan names the effect of the signal within each band as primary, on the
        # seconds over the limit and on the number of scale-ups.
        "jobs": [("builtin", "Stock band", VIOLET, "v"), ("builtin+lagfix", "Stock band, broker signal", MAGENTA, "s"),
                 ("builtin+max70", "Matched band", BLUE, "o"), ("builtin+max70+lagfix", "Matched band, broker signal", AQUA, "D")],
        "pairs": [("Stock band, broker signal", "Stock band"), ("Matched band, broker signal", "Matched band")],
        "primary": [("Stock band, broker signal", "Stock band"), ("Matched band, broker signal", "Matched band")],
        "primary_cols": ["slo_violation_s", "scale_ups"],
    },
    "e2b": {
        # The noise floor in the sustained overload: four identical policy-in-code jobs.
        "root": "results-e2b", "arms": {"aa-rule": "identical policy-in-code jobs, sustained overload"}, "pool": False,
        "jobs": [("rule+a", "PaC a", YELLOW, "o"), ("rule+b", "PaC b", YELLOW, "s"), ("rule+c", "PaC c", YELLOW, "D"),
                 ("rule+d", "PaC d", YELLOW, "^")],
    },
    "e2c": {
        # The noise floor in the three short scenarios that E2 does not cover (skew, ramp, drop): four
        # identical policy-in-code jobs, repetitions 1 to 8 (plan entry of 2026-11-22 09:54 PST).
        "root": "results-e2c", "arms": {"aa-rule": "identical policy-in-code jobs, skew, ramp and drop"}, "pool": False,
        "jobs": [("rule+a", "PaC a", YELLOW, "o"), ("rule+b", "PaC b", YELLOW, "s"), ("rule+c", "PaC c", YELLOW, "D"),
                 ("rule+d", "PaC d", YELLOW, "^")],
    },
    "e3b": {
        # The shipped band next to the band at the target, the policy in code and the LLM in the overload.
        "root": "results-e3b", "arms": {"default": "sustained overload, default signal"}, "pool": False,
        "reference": "builtin",
        "jobs": [("builtin", "Builtin-stock", VIOLET, "v"), ("builtin+max70", "Builtin", BLUE, "o"),
                 ("rule", "PaC", YELLOW, "D"), ("llm", "LLM", AQUA, "^")],
        "pairs": [("Builtin", "Builtin-stock"), ("PaC", "Builtin-stock"), ("LLM", "Builtin-stock"), ("LLM", "PaC")],
    },
    "e8s": {
        # The shipped band with the default signal, the broker-side signal and two smaller fetch sizes.
        "root": "results-e8s", "arms": {"default": "shipped band"},
        # Part B of the plan: the primary tests pool the three short scenarios; the overload (part A) has no test.
        "pool_scenarios": ["spike", "ramp", "capacity"],
        "primary": [("Broker-side signal", "Default signal")], "primary_cols": ["slo_violation_s", "scale_ups"],
        "jobs": [("builtin", "Default signal", BLUE, "o"), ("builtin+lagfix", "Broker-side signal", AQUA, "D"),
                 ("builtin+f65536", "64 KiB fetch", ORANGE, "^"), ("builtin+f4096", "4 KiB fetch", YELLOW, "s")],
    },
    "e3": {
        "root": "results-e3", "arms": {"default": "default backlog signal"},
        # Shown from the stock band to the policy in code; compared with the matched band.
        "reference": "builtin+max70",
        "jobs": [("builtin", "Builtin-stock", VIOLET, "v"), ("builtin+max80+min50", "Builtin-narrow", MAGENTA, "s"),
                 ("builtin+max70", "Builtin", BLUE, "o"), ("rule", "PaC", YELLOW, "D")],
    },
    "e5": {
        "root": "results-e5",
        "arms": {"default": "held-out workloads", "accurate": "held-out workloads, fresh signal",
                 "long": "held-out workloads, 5-minute windows"},
        "primary": MODEL_CONTRASTS,
        "jobs": [("builtin+max70", "Builtin", BLUE, "o"), ("rule", "PaC", YELLOW, "D"), ("jev", "DM", ORANGE, "s"),
                 ("llm", "LLM", AQUA, "^")],
    },
    "e9": {
        "root": "results-e9", "arms": {"error": "model calls fail", "hang": "model calls hang"},
        "primary": MODEL_CONTRASTS,
        "jobs": [("builtin+max70", "Builtin", BLUE, "o"), ("rule", "PaC", YELLOW, "D"), ("jev", "DM", ORANGE, "s"),
                 ("llm", "LLM", AQUA, "^")],
    },
    "e4": {
        "root": "results-e4", "arms": {"default": "restart 60 s longer"},
        "primary": MODEL_CONTRASTS,
        "jobs": [("builtin+max70+rd60+rt70", "Builtin", BLUE, "o"), ("rule+rd60+rt70", "PaC", YELLOW, "D"),
                 ("jev+rd60+rt70", "DM", ORANGE, "s"), ("llm+rd60+rt70", "LLM", AQUA, "^")],
    },
    "e2": {
        "root": "results-e2",
        "arms": {"aa-builtin": "identical built-in jobs", "aa-rule": "identical policy-in-code jobs",
                 "solo-builtin": "built-in alone", "solo-rule": "policy in code alone"},
        "jobs": [("builtin+max70+a", "Builtin a", BLUE, "o"), ("builtin+max70+b", "Builtin b", BLUE, "s"),
                 ("builtin+max70+noplugin+c", "Builtin c (no plugin)", VIOLET, "o"),
                 ("builtin+max70+noplugin+d", "Builtin d (no plugin)", VIOLET, "s"),
                 ("rule+a", "PaC a", YELLOW, "o"), ("rule+b", "PaC b", YELLOW, "s"), ("rule+c", "PaC c", YELLOW, "D"),
                 ("rule+d", "PaC d", YELLOW, "^"), ("builtin+max70", "Builtin alone", BLUE, "D"), ("rule", "PaC alone", YELLOW, "v")],
    },
}
# Set by main() for the campaign being analysed.
ARMS, ARM_NAME, JOB_ORDER, NAME, MARK, COLOR = (), {}, [], {}, {}, {}
REFERENCE, PAIRS, PRIMARY_PAIRS, PRIMARY_COLS, POOLED, ALIAS = None, [], [], [], {}, {}
POOL = True  # False for campaigns whose plan tests each scenario on its own: no pooled contrast is computed
POOL_SCENARIOS = None  # the scenarios a campaign's plan pools over, when not all of them (E8s: the three short ones)
PRIMARY = [("slo_violation_s", "seconds over the 2 s limit"), ("slots_mean", "slots in use")]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
TARGET_UTIL = 0.7
MAX_PAR = 8
RNG = np.random.default_rng(20261117)

plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": MUTED, "axes.labelcolor": INK, "xtick.color": INK, "ytick.color": INK, "text.color": INK,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.5, "axes.spines.top": False,
    "axes.spines.right": False, "font.size": 7.5, "axes.titlesize": 8, "axes.labelsize": 7.5,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7, "legend.frameon": False,
    "lines.linewidth": 1.2, "pdf.fonttype": 42, "font.family": "DejaVu Sans",
})


def present(df, extra=None):
    """Scenarios that have data, in the paper's order, followed by any others (such as a trace replay)."""
    d = df if extra is None else df[extra]
    found = list(dict.fromkeys(d.scenario))
    return [s for s in ORDER if s in found] + [s for s in found if s not in ORDER]


# ------------------------------------------------------------------ per-run metrics

def demand_parallelism(load):
    """Fewest Aggregate subtasks that keep utilization at or below the target, per load sample."""
    need = np.ceil(load.target_rate * load.agg_cost_us / 1e6 / TARGET_UTIL - 1e-9)
    return need.clip(1, MAX_PAR)


def elasticity(samples, load, job, dur):
    """SPEC RG elasticity metrics (Herbst et al.) for the Aggregate vertex over the measured phase:
    accuracy = average relative amount, timeshare = fraction of time, under- or over-provisioned."""
    s = samples[(samples.job == job) & (samples.t >= 0) & (samples.t <= dur)]
    supply = s.p_aggregate.ffill().bfill().to_numpy(float)
    demand = np.interp(s.t.to_numpy(float), load.t.to_numpy(float), demand_parallelism(load).to_numpy(float))
    demand = np.ceil(demand - 1e-9)
    if len(supply) == 0:
        return {}
    under, over = np.clip(demand - supply, 0, None), np.clip(supply - demand, 0, None)
    return {
        "under_accuracy_pct": float(100 * np.mean(under / demand)),
        "over_accuracy_pct": float(100 * np.mean(over / demand)),
        "under_timeshare_pct": float(100 * np.mean(under > 0)),
        "over_timeshare_pct": float(100 * np.mean(over > 0)),
    }


def beliefs(meta, samples, load, decisions, job):
    """What the autoscaler believed at each cycle (decision log) next to what was true (Kafka, load
    generator), for the source vertex. Times in seconds since the measured phase began."""
    rows = []
    s = samples[samples.job == job]
    conf = (meta.get("jobs_conf") or {}).get(job) or meta.get("autoscaler") or {}
    window = analyze.seconds(conf.get("job.autoscaler.metrics.window", "1m"))
    lag_t = s.t.to_numpy(float)
    if "position_offset" in s and s.position_offset.notna().any():
        # Broker end offsets minus the reader's position, at the time the position was taken.
        lag_t, lag_true = analyze.true_backlog(s, meta)
    else:
        # Older runs: committed offsets, one checkpoint old on average, minus what was read meanwhile.
        read = s.out_rate_source.fillna(0).to_numpy(float) if "out_rate_source" in s else 0
        lag_true = np.clip((s.end_offset - s.committed_offset).to_numpy(float) - 5.0 * read, 0, None)
    for d in decisions:
        if d.get("event") != "decision" or f"name='bench-{analyze.safe(job)}'" not in d.get("job", ""):
            continue
        src = next((v for v in d.get("vertices", {}).values() if str(v.get("name", "")).startswith("Source")), None)
        if not src or src.get("demand") is None:
            continue
        ts = datetime.fromisoformat(d["ts"].replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
        t = ts - meta["t0_epoch"]
        agg = next((v for v in d.get("vertices", {}).values() if str(v.get("name", "")).startswith("Aggregate")), {})
        rows.append({
            "t": t, "believed_demand": src["demand"], "believed_backlog": src.get("backlog") or 0.0,
            "true_rate": float(np.interp(t, load.t, load.target_rate)),
            # The rule's demand is an average over its metric window, so the like-for-like truth is
            # the arrival rate averaged over the same window.
            "true_rate_window": float(np.mean(np.interp(np.linspace(t - window, t, 121), load.t, load.target_rate))),
            "true_backlog": float(np.interp(t, lag_t, lag_true)) if len(s) else np.nan,
            # A second truth that does not use the readers' position gauges: end offsets minus
            # committed offsets. It is an upper bound, up to one checkpoint interval too high.
            "bound_backlog": float(np.interp(t, s.t.to_numpy(float), (s.end_offset - s.committed_offset.ffill()).to_numpy(float)))
            if len(s) and "committed_offset" in s else np.nan,
            "agg_demand": agg.get("demand"), "agg_backlog": agg.get("backlog") or 0.0,
            "agg_capacity": agg.get("capacity_per_subtask"), "agg_parallelism": agg.get("current"),
        })
    return pd.DataFrame(rows)


ALIGNED = [p for p in range(1, MAX_PAR + 1) if 120 % p == 0]  # divisors of the job's 120 key groups


# The operator's default time constants, for "what would the same inputs give with the defaults".
DEFAULT_TIMES = {"job.autoscaler.utilization.target": "0.7", "job.autoscaler.catch-up.duration": "30m",
                 "job.autoscaler.restart.time": "5m"}


def formula_need(demand, backlog, capacity, conf):
    """Equation (1) of the paper before rounding: the number of subtasks the built-in rule's formula
    gives a vertex for these inputs, not aligned and not limited."""
    target = float(conf.get("job.autoscaler.utilization.target", TARGET_UTIL))
    catch_up = analyze.seconds(conf.get("job.autoscaler.catch-up.duration", "1m"))
    restart = analyze.seconds(conf.get("job.autoscaler.restart.time", "30s"))
    if not capacity or capacity <= 0 or demand is None:
        return np.nan
    return float((demand / target + backlog / catch_up + demand * restart / catch_up) / capacity)


def formula_size(demand, backlog, capacity, conf):
    """Equation (1) of the paper: the parallelism the built-in rule gives a vertex for these inputs,
    aligned upwards to the key groups and limited to the maximum."""
    need = formula_need(demand, backlog, capacity, conf)
    if np.isnan(need):
        return np.nan
    n = int(np.ceil(need - 1e-9))
    return float(next((p for p in ALIGNED if p >= n), MAX_PAR))


def scale_ups(meta, samples, load, decisions, job):
    """Every scale-up of Aggregate after the first workload change, one row each: when it was asked
    for, from and to which parallelism, and what Equation (1) gives for the deciding cycle with what
    the autoscaler believed, with the true backlog and the true arrival rate averaged over the metric
    window (what the same rule computes from a correct signal), and with the true backlog and the
    arrival rate of that moment. The capacity per subtask is the believed one in all three."""
    s = samples[samples.job == job]
    asked = pd.to_numeric(s.target_p_aggregate, errors="coerce").ffill()
    asked = asked.fillna(s.p_aggregate.ffill().bfill())
    events = meta.get("events") or [0]
    first_event = events[0]
    ups = s.t[(asked.diff() > 0) & (s.t >= first_event) & (s.t <= meta["duration"])]
    b = beliefs(meta, samples, load, decisions, job)
    conf = (meta.get("jobs_conf") or {}).get(job) or meta.get("autoscaler") or {}
    window = analyze.seconds(conf.get("job.autoscaler.metrics.window", "1m"))
    rows = []
    for t_up in ups:
        t_up = float(t_up)
        row = {"t": t_up, "since_event": t_up - first_event,
               "from": float(asked[s.t < t_up].iloc[-1]) if (s.t < t_up).any() else np.nan,
               "to": float(asked[s.t >= t_up].iloc[0])}
        # The cycle that decided it: the last one logged before the change shows up in the samples.
        # A job that takes long to come back shows its new parallelism late, so look further back
        # when no cycle is that close.
        cycle = b[(b.t <= t_up + 1) & (b.t >= t_up - 25)] if len(b) else b
        if len(b) and not len(cycle):
            cycle = b[(b.t <= t_up + 1) & (b.t >= t_up - 120)]
        if len(cycle):
            c = cycle.iloc[-1]
            # Was the whole metric window of this cycle inside the changed workload?
            inside = c.t - window >= first_event and (len(events) < 2 or c.t <= events[1])
            row.update({
                "cycle_t": float(c.t), "full_window": bool(inside),
                "believed_demand": c.agg_demand, "believed_backlog": c.agg_backlog, "believed_capacity": c.agg_capacity,
                "true_rate": c.true_rate, "true_rate_window": c.true_rate_window, "true_backlog": c.true_backlog,
                "bound_backlog": c.get("bound_backlog", np.nan),
                "need_window": formula_need(c.true_rate_window, c.true_backlog, c.agg_capacity, conf),
                "need_believed": formula_need(c.agg_demand, c.agg_backlog, c.agg_capacity, conf),
                "need_demand": formula_need(c.true_rate_window, c.agg_backlog, c.agg_capacity, conf),
                "need_backlog": formula_need(c.agg_demand, c.true_backlog, c.agg_capacity, conf),
                "dneed_believed": formula_need(c.agg_demand, c.agg_backlog, c.agg_capacity, DEFAULT_TIMES),
                "dneed_demand": formula_need(c.true_rate_window, c.agg_backlog, c.agg_capacity, DEFAULT_TIMES),
                "dneed_backlog": formula_need(c.agg_demand, c.true_backlog, c.agg_capacity, DEFAULT_TIMES),
                "dneed_both": formula_need(c.true_rate_window, c.true_backlog, c.agg_capacity, DEFAULT_TIMES),
                "size_believed": formula_size(c.agg_demand, c.agg_backlog, c.agg_capacity, conf),
                "size_window": formula_size(c.true_rate_window, c.true_backlog, c.agg_capacity, conf),
                "size_true": formula_size(c.true_rate, c.true_backlog, c.agg_capacity, conf),
            })
        rows.append(row)
    return pd.DataFrame(rows)


def demand_phases(meta, samples, load, decisions, job, floor=5000.0):
    """Believed over true demand (window averages) in the cycles whose metric window lies inside the
    overload and that have a real backlog, split by whether Aggregate already had the parallelism
    that keeps up with the load. One row per cycle."""
    events = meta.get("events") or [0]
    if len(events) < 2:
        return pd.DataFrame()
    b = beliefs(meta, samples, load, decisions, job)
    if not len(b):
        return b
    conf = (meta.get("jobs_conf") or {}).get(job) or meta.get("autoscaler") or {}
    window = analyze.seconds(conf.get("job.autoscaler.metrics.window", "1m"))
    b = b[(b.t - window >= events[0]) & (b.t <= events[1]) & (b.true_backlog > floor) & (b.true_rate_window > 0)].copy()
    if not len(b):
        return b
    b["ratio"] = b.believed_demand / b.true_rate_window
    # The load needs this many subtasks at the capacity the rule measured in that cycle.
    b["keeps_up"] = pd.to_numeric(b.agg_parallelism, errors="coerce") * pd.to_numeric(b.agg_capacity, errors="coerce") \
        > b.true_rate_window
    return b[["t", "ratio", "keeps_up"]]


def sizing(meta, samples, load, decisions, job):
    """Run-level numbers on the scale-ups of Aggregate: the first one (as the analysis plan names it),
    those decided with a metric window that lay wholly inside the changed workload, and the time
    until the job had the capacity for the load."""
    ups = scale_ups(meta, samples, load, decisions, job)
    out = {"scale_ups": int(len(ups))}
    events = meta.get("events") or [0]
    if len(events) > 1:
        out["overload_end_s"] = float(events[1])
    s = samples[(samples.job == job) & (samples.t >= events[0]) & (samples.t <= meta["duration"])]
    # Time until the running parallelism of Aggregate covers the highest load of the scenario, at
    # the nominal capacity of a subtask (the same number for all jobs of a run).
    if len(s):
        scenario = scenarios.get(meta["scenario"])
        grid = np.arange(events[0], meta["duration"] + 1.0)
        cost = np.array([scenario.agg_cost(t - meta.get("shift", 0.0)) for t in grid])
        needed = int(np.ceil(np.max(np.interp(grid, load.t, load.target_rate) * cost / 1e6) - 1e-9))
        running = s.state == "RUNNING"
        enough = s.t[(s.p_aggregate.ffill() >= needed) & running]
        out["needed_parallelism"] = needed
        out["time_to_capacity_s"] = float(enough.iloc[0] - events[0]) if len(enough) else np.nan
        # Time until Aggregate runs at the size that Equation (1) gives for the highest load of the
        # scenario with true inputs: the true rate, no backlog, the nominal capacity of a subtask.
        # This is the size the rule itself aims for, the same for all jobs of a run.
        conf = (meta.get("jobs_conf") or {}).get(job) or meta.get("autoscaler") or {}
        # The hardest moment is where rate times cost per record is largest: a scenario may raise the
        # cost and leave the rate alone.
        rate = np.interp(grid, load.t, load.target_rate)
        hardest = int(np.argmax(rate * cost))
        full = formula_size(float(rate[hardest]), 0.0, 1e6 / float(cost[hardest]), conf)
        at_full = s.t[(s.p_aggregate.ffill() >= full) & running]
        out["full_size"] = full
        out["time_to_full_s"] = float(at_full.iloc[0] - events[0]) if len(at_full) else np.nan
    if not len(ups) or "size_window" not in ups:
        return out
    first = ups.iloc[0]
    out.update({
        "first_up_s": first.since_event, "first_up_from": first["from"], "first_up_to": first.to,
        "first_up_believed": first.get("size_believed", np.nan), "first_up_window": first.get("size_window", np.nan),
        "first_up_true": first.get("size_true", np.nan),
        "ups_too_small": int((ups.to < ups.size_window).sum()),
        "ups_checked": int(ups.size_window.notna().sum()),
    })
    late = ups[ups.full_window == True]  # noqa: E712
    out["late_ups"] = int(len(late))
    if len(late):
        out.update({
            "late_demand_seen": float((late.believed_demand / late.true_rate_window).mean()),
            # Only where a real backlog existed (10 s of input), as in belief_metrics().
            "late_backlog_seen": float((late.believed_backlog / late.true_backlog)[late.true_backlog >= 10 * late.true_rate_window].mean()),
            "late_asked": float(late.to.mean()), "late_window": float(late.size_window.mean()),
            "late_too_small": int((late.to < late.size_window).sum()),
        })
    return out


def fault_metrics(meta, decisions, job):
    """Fault-injection runs: what the job's autoscaler did while model calls failed. The plugin logs
    one decision per cycle in which the autoscaler has a full metric window, also for the built-in
    job (observe-only). A fallback is a cycle that the plugin handed to the built-in rule: after a
    failed call, or without a call while the circuit breaker is open."""
    fault = meta.get("fault")
    if not fault:
        return {}
    cycles = []
    for d in decisions:
        if d.get("event") == "decision" and f"name='bench-{analyze.safe(job)}'" in d.get("job", ""):
            ts = datetime.fromisoformat(d["ts"].replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
            cycles.append((ts - meta["t0_epoch"], str(d.get("status")), str(d.get("reason") or "")))
    cycles.sort()
    t = np.array([c[0] for c in cycles])
    gaps = np.diff(t)

    def interval(lo, hi):
        # A scaling action is followed by about 105 s without a decision, so only gaps under a minute
        # are intervals between consecutive cycles.
        g = gaps[(t[1:] >= lo) & (t[1:] <= hi) & (gaps < 60)]
        return float(np.median(g)) if len(g) else np.nan

    inside = [c for c in cycles if fault["from"] <= c[0] <= fault["until"]]
    after = [c for c in cycles if c[0] > fault["until"]]
    recovered = next((c[0] for c in after if c[1] != "FALLBACK"), np.nan)
    return {
        "fault_cycles": len(inside),
        "fault_fallbacks": sum(1 for c in inside if c[1] == "FALLBACK"),
        "fault_failed_calls": sum(1 for c in inside if c[1] == "FALLBACK" and c[2].startswith("model_error")),
        "fault_circuit_open": sum(1 for c in inside if c[1] == "FALLBACK" and c[2] == "circuit_open"),
        "fault_interval_s": interval(fault["from"], fault["until"]),
        "calm_interval_s": interval(-1e9, fault["from"]),
        "fallbacks_after_fault": sum(1 for c in after if c[1] == "FALLBACK"),
        "model_back_after_s": recovered - fault["until"],
    }


def fault_numbers(df, numbers, tag):
    """The fault-injection campaign: per kind of fault and controller, the cycles during the fault,
    how many of them fell back to the built-in rule, the time between cycles, and the outcome next
    to the same scenario without a fault (main campaign, default arm; other runs, so unpaired)."""
    if "fault_cycles" not in df:
        return ""
    out = ["=== model calls failing during the scenario"]
    base_path = os.path.join(ROOT, EXPERIMENTS["main"]["root"], "runs.csv")
    base = pd.read_csv(base_path) if os.path.exists(base_path) else None
    for arm in [a for a in ARMS if (df.arm == a).any()]:
        for job in [j for j in JOB_ORDER if ((df.arm == arm) & (df.job == j)).any()]:
            d = df[(df.arm == arm) & (df.job == job)]
            key = (tag, "fault", arm, NAME[job])
            for col, name in (("fault_cycles", "cycles"), ("fault_fallbacks", "fallbacks"), ("fault_failed_calls", "failed calls"),
                              ("fault_circuit_open", "circuit open"), ("fallbacks_after_fault", "fallbacks after")):
                numbers.add(macro_name(*key, name), f"{int(d[col].sum()):,}")
            for col, name in (("fault_interval_s", "interval"), ("calm_interval_s", "calm interval"),
                              ("model_back_after_s", "model back"), ("first_up_s", "first up"),
                              ("slo_violation_s", "slo"), ("slots_mean", "slots"), ("scale_ups", "ups")):
                if col in d:
                    numbers.add(macro_name(*key, name), num(float(d[col].mean())))
            line = (f"  {arm:6s} {NAME[job]:8s} n={len(d)}: {int(d.fault_cycles.sum())} cycles in the fault, "
                    f"{int(d.fault_fallbacks.sum())} fallbacks ({int(d.fault_failed_calls.sum())} failed calls, "
                    f"{int(d.fault_circuit_open.sum())} circuit open); cycle every {num(float(d.fault_interval_s.mean()))} s "
                    f"(before the fault {num(float(d.calm_interval_s.mean()))} s); first scale-up after "
                    f"{num(float(d.first_up_s.mean())) if 'first_up_s' in d else '--'} s; "
                    f"{num(float(d.slo_violation_s.mean()))} s over the limit, {num(float(d.slots_mean.mean()))} slots")
            if base is not None:
                b = base[(base.arm == "default") & (base.job == job) & base.scenario.isin(set(d.scenario))]
                if len(b):
                    numbers.add(macro_name(*key, "no fault slo"), num(float(b.slo_violation_s.mean())))
                    numbers.add(macro_name(*key, "no fault slots"), num(float(b.slots_mean.mean())))
                    numbers.add(macro_name(*key, "no fault runs"), str(len(b)))
                    line += f"; without a fault {num(float(b.slo_violation_s.mean()))} s, {num(float(b.slots_mean.mean()))} slots (n={len(b)})"
            out.append(line)
    return "\n".join(out)


def _when(d, meta):
    """Seconds since the measured phase began for one line of the decision log."""
    return datetime.fromisoformat(d["ts"].replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() - meta["t0_epoch"]


def _aggregate(d):
    return next((v for v in (d.get("vertices") or {}).values() if str(v.get("name", "")).startswith("Aggregate")), None)


def observe_agreement(meta, samples, mine, job):
    """A built-in job runs the plugin in observe-only mode, which logs for every cycle what the policy
    in code would require (its trigger and its size) and changes nothing. This compares that with
    what the built-in rule did in the same cycle, for Aggregate: the parallelism asked for between
    this cycle and the next. The policy's class leaves out its cooldown for scaling down, which the
    observe-only plugin does not track."""
    s = samples[samples.job == job]
    if not len(s) or "target_p_aggregate" not in s:
        return {}
    asked = pd.to_numeric(s.target_p_aggregate, errors="coerce").ffill().fillna(s.p_aggregate.ffill().bfill()).to_numpy(float)
    ts = s.t.to_numpy(float)
    cycles = sorted(((_when(d, meta), _aggregate(d)) for d in mine), key=lambda c: c[0])
    out = {}
    for i, (t, v) in enumerate(cycles):
        if not v or v.get("rule_trigger") is None or v.get("rule_size") is None or not v.get("current"):
            continue
        if not 0 <= t <= meta["duration"]:
            continue
        cur, trigger, size = int(v["current"]), int(v["rule_trigger"]), int(v["rule_size"])
        top = min(max(cur + 1, 4 * cur), MAX_PAR)
        # The policy's choice among the options it would have been offered (DecisionEngine.ruleChoice).
        # At the maximum parallelism it can only hold, whatever the trigger says.
        if cur < trigger:
            want = next((p for p in ALIGNED if cur < p <= top and p >= size), max(p for p in ALIGNED + [cur] if p <= top))
        elif cur - size >= 2:
            want = next((p for p in ALIGNED if p >= size), cur)
        else:
            want = cur
        cls = "up" if want > cur else "down" if want < cur else "hold"
        until = min(cycles[i + 1][0] - 1 if i + 1 < len(cycles) else t + 120, t + 120)
        after = asked[(ts > t - 1) & (ts <= until)]
        changed = after[after != cur]
        did = int(changed[0]) if len(changed) else cur
        way = "up" if did > cur else "down" if did < cur else "hold"
        out[f"obs_{cls}_n"] = out.get(f"obs_{cls}_n", 0) + 1
        out[f"obs_{cls}_same_dir"] = out.get(f"obs_{cls}_same_dir", 0) + int(way == cls)
        out[f"obs_{cls}_same_size"] = out.get(f"obs_{cls}_same_size", 0) + int(did == want)
        out[f"obs_{cls}_smaller"] = out.get(f"obs_{cls}_smaller", 0) + int(way == cls and did < want)
        out[f"obs_{cls}_larger"] = out.get(f"obs_{cls}_larger", 0) + int(way == cls and did > want)
    return out


def decision_metrics(meta, samples, load, decisions, job):
    """Per-run counts for the comparison of decisions (analysis plan, amendments of 2026-11-17 20:15
    and 2026-11-18 10:00 PST). For a job with the policy in code or a model, per vertex decision:
    the class is what the policy chose (hold, up, down); agreement is the model's most likely option
    being the policy's. For Aggregate, a deviation is scored against the size that Equation (1) gives
    with the true arrival rate of that moment and the true backlog. Latitude: cycles in which the
    prompt's own example holds for Aggregate (busy time at least 95% and believed demand below the
    capacity). A step is capped when the policy's size lies above the largest option offered and that
    option is the limit of four times the current parallelism, not the maximum parallelism."""
    mine = [d for d in decisions if d.get("event") == "decision" and f"name='bench-{analyze.safe(job)}'" in d.get("job", "")]
    mine = [d for d in mine if 0 <= _when(d, meta) <= meta["duration"]]
    if not mine:
        return {}
    out = {"cycles": len(mine)}
    if job.split("+")[0] == "builtin":
        out.update(observe_agreement(meta, samples, mine, job))
        return out
    status = [(str(d.get("status")), str(d.get("reason") or "")) for d in mine]
    out.update({
        "cycles_fallback": sum(1 for st, _ in status if st == "FALLBACK"),
        "cycles_failed_call": sum(1 for st, r in status if st == "FALLBACK" and r.startswith("model_error")),
        "cycles_circuit_open": sum(1 for st, r in status if st == "FALLBACK" and r == "circuit_open"),
        "cycles_cooldown": sum(1 for st, _ in status if st == "COOLDOWN"),
    })
    conf = (meta.get("jobs_conf") or {}).get(job) or meta.get("autoscaler") or {}
    b = beliefs(meta, samples, load, decisions, job)
    keys = ["dec_hold_n", "dec_hold_agree", "dec_up_n", "dec_up_agree", "dec_down_n", "dec_down_agree",
            "dev_n", "dev_towards", "dev_away", "dev_same", "dev_unknown", "dev_above", "dev_below",
            "lat_cycles", "lat_policy_up", "lat_above", "lat_above_subtasks",
            "olat_cycles", "olat_policy_up", "olat_above", "olat_above_subtasks", "lat_at_max",
            "policy_ups", "policy_ups_capped", "policy_ups_doubled", "top_chosen", "capped_total",
            "agg_n", "agg_agree", "agg_applied_agree", "oth_n", "oth_agree", "oth_applied_agree"]
    out.update({k: 0 for k in keys})
    for d in mine:
        t = _when(d, meta)
        for v in (d.get("vertices") or {}).values():
            if str(v.get("reason") or "") in ("capped_by_total_limit", "model_scale_up_capped"):
                out["capped_total"] += 1
            if not v.get("choice") or v.get("rule_choice") is None or not v.get("current"):
                # The prompt's situation while Aggregate runs at the maximum: one size on offer, no model asked.
                # A job that drains a backlog looks like this too.
                if str(v.get("name", "")).startswith("Aggregate") and int(v.get("current") or 0) >= MAX_PAR \
                        and v.get("busy") is not None and v.get("capacity_per_subtask") and v.get("demand") is not None \
                        and float(v["busy"]) >= 0.95 \
                        and float(v["demand"]) < float(v["capacity_per_subtask"]) * int(v["current"]):
                    out["lat_at_max"] += 1
                continue
            cur, rule, choice = int(v["current"]), int(v["rule_choice"]), int(str(v["choice"])[1:])
            cls = "hold" if rule == cur else "up" if rule > cur else "down"
            out[f"dec_{cls}_n"] += 1
            out[f"dec_{cls}_agree"] += int(choice == rule)
            part = "agg" if str(v.get("name", "")).startswith("Aggregate") else "oth"
            out[f"{part}_n"] += 1
            out[f"{part}_agree"] += int(choice == rule)
            out[f"{part}_applied_agree"] += int(int(v.get("target") or cur) == rule)
            if part != "agg":
                # The prompt's situation on a vertex other than Aggregate: not in the plan's count.
                capacity = v.get("capacity_per_subtask")
                if v.get("busy") is not None and capacity and v.get("demand") is not None \
                        and float(v["busy"]) >= 0.95 and float(v["demand"]) < float(capacity) * cur:
                    out["olat_cycles"] += 1
                    out["olat_policy_up"] += int(rule > cur)
                    out["olat_above"] += int(choice > rule)
                    out["olat_above_subtasks"] += max(choice - rule, 0)
                continue
            top = int(str(v.get("offered") or f"p{cur}..p{cur}").split("..p")[-1])
            limited = top < MAX_PAR  # the largest option is the limit per step, not the maximum parallelism
            if rule > cur:
                out["policy_ups"] += 1
                out["policy_ups_capped"] += int(limited and int(v.get("rule_size") or 0) > top)
                # The projection may at most double the demand; 1.9 allows for rounding in the log.
                seen, ahead = float(v.get("demand") or 0), float(v.get("projected_demand") or 0)
                out["policy_ups_doubled"] += int(seen > 0 and ahead / seen >= 1.9)
            out["top_chosen"] += int(limited and choice == top and choice > cur)
            capacity = v.get("capacity_per_subtask")
            if v.get("busy") is not None and capacity and v.get("demand") is not None \
                    and float(v["busy"]) >= 0.95 and float(v["demand"]) < float(capacity) * cur:
                out["lat_cycles"] += 1
                out["lat_policy_up"] += int(rule > cur)
                out["lat_above"] += int(choice > rule)
                out["lat_above_subtasks"] += max(choice - rule, 0)
            if choice == rule:
                continue
            out["dev_n"] += 1
            out["dev_above" if choice > rule else "dev_below"] += 1
            row = b[(b.t - t).abs() < 0.5] if len(b) else b
            need = formula_size(float(row.iloc[0].true_rate), float(row.iloc[0].true_backlog), capacity, conf) \
                if len(row) and capacity else np.nan
            if np.isnan(need):
                out["dev_unknown"] += 1
            elif abs(choice - need) < abs(rule - need):
                out["dev_towards"] += 1
            elif abs(choice - need) > abs(rule - need):
                out["dev_away"] += 1
            else:
                out["dev_same"] += 1
    return out


def belief_metrics(b, dur):
    """How far the believed backlog and demand were from the truth while a real backlog existed
    (more than 10 s of input waiting). The believed demand is an average over the metric window and
    is compared with the true arrival rate averaged over the same window."""
    if not len(b):
        return {}
    w = b[(b.t >= 0) & (b.t <= dur) & (b.true_backlog > 10 * b.true_rate_window)]
    if not len(w):
        return {"overload_cycles": 0}
    return {
        "overload_cycles": int(len(w)),
        "backlog_seen_ratio": float(np.median(w.believed_backlog / w.true_backlog)),
        "demand_seen_ratio": float(np.median(w.believed_demand / w.true_rate_window)),
    }


def renamed(run, alias):
    """A run with its jobs renamed in every part: the metadata, the samples, the latencies and the
    decision log. For an arm whose jobs carry an option in their names."""
    meta, samples, latency, load, decisions = run
    if not alias or not any(j in alias for j in meta["jobs"]):
        return run
    meta = dict(meta)
    meta["jobs"] = [alias.get(j, j) for j in meta["jobs"]]
    for key in ("jobs_conf", "lag_at_t0", "initial_parallelism"):
        if isinstance(meta.get(key), dict):
            meta[key] = {alias.get(j, j): v for j, v in meta[key].items()}
    samples, latency = samples.copy(), latency.copy()
    samples["job"] = samples.job.replace(alias)
    latency["job"] = latency.job.replace(alias)
    out = []
    for d in decisions:
        text = d.get("job", "")
        for old, new in alias.items():
            text = text.replace(f"name='bench-{analyze.safe(old)}'", f"name='bench-{analyze.safe(new)}'")
        out.append({**d, "job": text})
    return meta, samples, latency, load, out


def jm_restarts(path, meta):
    """JobManager containers that restarted during the measured phase, per job (Issue #61: the
    kernel killed JobManagers at their container's memory limit). A restart shows as a repeated
    'Started' event on the same JobManager pod; an in-place rescale does not restart the container
    and a redeployment creates a new pod. NaN if the run's events were not saved."""
    pods = {j: re.compile(rf"bench-{re.escape(analyze.safe(j))}-[0-9a-f]{{8,10}}-[a-z0-9]{{5}}") for j in meta["jobs"]}
    out, known = {j: 0 for j in meta["jobs"]}, False
    try:
        items = json.load(open(os.path.join(path, "events.json")))["items"]
        known = True
    except (OSError, ValueError, KeyError):
        items = []
    for e in items:
        o = e.get("involvedObject", {})
        if o.get("kind") != "Pod" or e.get("reason") != "Started" or (e.get("count") or 1) < 2 or not e.get("lastTimestamp"):
            continue
        t = datetime.fromisoformat(e["lastTimestamp"].replace("Z", "+00:00")).timestamp() - meta["t0_epoch"]
        if 0 <= t <= meta["duration"]:
            for j, pattern in pods.items():
                if pattern.fullmatch(o.get("name", "")):
                    out[j] += e["count"] - 1
    # The pod snapshot that newer runs save has the restart count of every container alive at the
    # end of the run, whenever the restart happened (events are dropped after about an hour).
    try:
        snapshot = json.load(open(os.path.join(path, "pods.json")))["items"]
        known = True
    except (OSError, ValueError, KeyError):
        snapshot = []
    for pod in snapshot:
        restarts = sum(c.get("restartCount", 0) for c in pod.get("status", {}).get("containerStatuses", []))
        for j, pattern in pods.items():
            if pattern.fullmatch(pod.get("metadata", {}).get("name", "")):
                out[j] = max(out[j], restarts)
    return out if known else {j: np.nan for j in meta["jobs"]}


def other_restarts(path):
    """Restarts of containers that are not JobManagers (TaskManagers), from the pod snapshot of a
    run; NaN for runs from before the snapshot was saved."""
    try:
        snapshot = json.load(open(os.path.join(path, "pods.json")))["items"]
    except (OSError, ValueError, KeyError):
        return np.nan
    return sum(c.get("restartCount", 0) for pod in snapshot if "taskmanager" in pod.get("metadata", {}).get("name", "")
               for c in pod.get("status", {}).get("containerStatuses", []))


def collect(root):
    records, runs, ups = [], {}, []
    for arm in ARMS:
        for meta_path in sorted(glob.glob(os.path.join(root, arm, "*", "rep*", "meta.json"))):
            path = os.path.dirname(meta_path)
            raw = analyze.load_run(path)
            killed = {ALIAS.get(j, j): n for j, n in jm_restarts(path, raw[0]).items()}
            run = renamed(raw, ALIAS)
            meta, samples, latency, load, decisions = run
            runs[(arm, meta["scenario"], meta["rep"])] = run
            for job in meta["jobs"]:
                m = analyze.metrics(*run, job)
                m["jm_restarts"] = killed.get(job, np.nan)
                m["tm_restarts_run"] = other_restarts(path)
                m.update(elasticity(samples, load, job, meta["duration"]))
                m.update(belief_metrics(beliefs(meta, samples, load, decisions, job), meta["duration"]))
                m.update(sizing(meta, samples, load, decisions, job))
                m.update(fault_metrics(meta, decisions, job))
                m.update(decision_metrics(meta, samples, load, decisions, job))
                ph = demand_phases(meta, samples, load, decisions, job)
                if len(ph):
                    m["demand_ratio_short"] = float(ph.ratio[~ph.keeps_up].median()) if (~ph.keeps_up).any() else np.nan
                    m["demand_ratio_drain"] = float(ph.ratio[ph.keeps_up].median()) if ph.keeps_up.any() else np.nan
                    m["demand_cycles_short"] = int((~ph.keeps_up).sum())
                    m["demand_cycles_drain"] = int(ph.keeps_up.sum())
                u = scale_ups(meta, samples, load, decisions, job)
                if len(u):
                    u.insert(0, "job", job)
                    u.insert(0, "rep", meta["rep"])
                    u.insert(0, "scenario", meta["scenario"])
                    u.insert(0, "arm", arm)
                    ups.append(u)
                records.append({"arm": arm, "scenario": meta["scenario"], "rep": meta["rep"], "job": job, **m})
    df = pd.DataFrame(records)
    if len(df):
        df["latency_p95_s"] = df.latency_p95_ms / 1000
        df.to_csv(os.path.join(root, "runs.csv"), index=False)
        if ups:
            # Every scale-up of Aggregate with the inputs of its deciding cycle, for the record.
            pd.concat(ups, ignore_index=True).round(2).to_csv(os.path.join(root, "scale_ups.csv"), index=False)
    return df, runs


# ------------------------------------------------------------------ statistics

def data_rng(x):
    """A random generator seeded from the data, so that one quantity gets one interval and one
    p-value wherever and in whatever order it is computed."""
    digest = hashlib.sha256(np.round(np.asarray(x, float), 9).tobytes()).digest()
    return np.random.default_rng(int.from_bytes(digest[:8], "little"))


def boot_ci(x, n=20000):
    """Mean and 95% interval of the mean: Student's t interval over the values, whatever their number,
    as the analysis plan says ("all intervals over runs are Student's t intervals"). Until 2026-11-22 this
    switched to a bootstrap interval (bias-corrected and accelerated) above 30 values; the main campaign
    was the first with more (40 pairs per arm), and for its pooled statistic that interval excluded zero
    in 14 to 20% of draws under the test's own null, the t interval in 2 to 3% (review round 3, R2,
    ISSUES.md #132). bca_ci() keeps the old interval for the record."""
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    if len(x) == 0:
        return np.nan, np.nan, np.nan
    m = float(x.mean())
    if len(x) == 1 or np.ptp(x) == 0:
        return m, m, m
    from scipy import stats
    half = float(stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / np.sqrt(len(x)))
    return m, m - half, m + half


def bca_ci(x, n=20000):
    """Mean and 95% bootstrap interval of the mean, bias-corrected and accelerated. Not used for any
    reported number since 2026-11-22; main_marked.py prints it next to the t interval."""
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    if len(x) < 3 or np.ptp(x) == 0:
        return boot_ci(x)
    m = float(x.mean())
    means = data_rng(x).choice(x, size=(n, len(x)), replace=True).mean(axis=1)
    normal = NormalDist()
    below = (np.sum(means < m) + 0.5 * np.sum(means == m)) / n
    z0 = normal.inv_cdf(min(max(below, 1 / (n + 1)), n / (n + 1)))
    jack = (x.sum() - x) / (len(x) - 1)  # leave-one-out means
    d = jack.mean() - jack
    denom = 6 * float(np.sum(d ** 2)) ** 1.5
    accel = float(np.sum(d ** 3)) / denom if denom > 0 else 0.0

    def adjusted(q):
        z = z0 + normal.inv_cdf(q)
        return 100 * normal.cdf(z0 + z / (1 - accel * z))

    lo, hi = np.percentile(means, [adjusted(0.025), adjusted(0.975)])
    return m, float(lo), float(hi)


def sign_flip_p(diff):
    """Two-sided sign-flip (permutation) p-value for the mean of paired differences. Exact up to 44
    pairs: the sums of the two halves of the sign patterns are enumerated and combined by a sorted
    search. Beyond that 200,000 random patterns, seeded from the data."""
    diff = np.asarray(diff, float)
    n, size = len(diff), np.abs(diff)
    if n <= 44:
        def sums(x):
            out = np.zeros(1)
            for v in x:
                out = np.concatenate([out + v, out - v])
            return out
        threshold = abs(diff.sum()) - 1e-12 * max(1.0, float(size.sum()))
        a, b = sums(size[: n // 2]), np.sort(sums(size[n // 2:]))
        count = int((len(b) - np.searchsorted(b, threshold - a, side="left")).sum())  # a + b >= threshold
        count += int(np.searchsorted(b, -threshold - a, side="right").sum())  # a + b <= -threshold
        return min(1.0, count / float(2 ** n))
    observed = abs(diff.mean()) - 1e-12
    rng, count, draws = data_rng(diff), 0, 200000
    for _ in range(20):
        signs = rng.choice([-1, 1], size=(draws // 20, n))
        count += int(np.sum(np.abs((signs * size).mean(axis=1)) >= observed))
    return (count + 1) / (draws + 1)


def paired(df, arm, scenario, a, b, col):
    """Mean of (a - b) over repetitions in which both jobs ran side by side, with a bootstrap interval
    and the exact two-sided sign-flip (permutation) p-value."""
    d = df[(df.arm == arm) & (df.scenario == scenario)].pivot(index="rep", columns="job", values=col)
    if a not in d or b not in d:
        return None
    diff = (d[a] - d[b]).dropna().to_numpy(float)
    if len(diff) < 2:
        return None
    mean, lo, hi = boot_ci(diff)
    n = len(diff)
    return {"n": n, "mean": mean, "lo": lo, "hi": hi, "p": sign_flip_p(diff), "wins": int((diff < 0).sum())}


POOL_FLOOR = {"slo_violation_s": 30.0, "slots_mean": 0.5, "scale_ups": 0.5}


def pooled(df, arm, a, b, col):
    """a minus b over all scenarios of an arm, as a fraction of the scenario's mean level so that no
    scenario dominates, with a bootstrap interval and a sign-flip test over all pairs."""
    diffs = []
    for sc in present(df, df.arm == arm):
        if POOL_SCENARIOS and sc not in POOL_SCENARIOS:
            continue
        d = df[(df.arm == arm) & (df.scenario == sc)].pivot(index="rep", columns="job", values=col)
        if a not in d or b not in d:
            continue
        x = (d[a] - d[b]).dropna().to_numpy(float)
        # The divisor has a floor, so that a scenario in which both jobs stay near zero does not
        # dominate the pooled figure in ratio (analysis plan, amendment of 2026-11-17).
        scale = max(np.nanmean(np.r_[d[a].to_numpy(float), d[b].to_numpy(float)]), POOL_FLOOR.get(col, 0.0))
        if len(x) and scale > 0:
            diffs += list(x / scale)
    diffs = np.array(diffs)
    if len(diffs) < 4:
        return None
    mean, lo, hi = boot_ci(diffs)
    return {"n": len(diffs), "mean": mean, "lo": lo, "hi": hi, "p": sign_flip_p(diffs)}


def identical_pairs(df):
    """Relative differences between identical jobs that ran side by side (the noise-floor campaign):
    one row per run, pair of jobs and primary metric. Jobs are identical if their specs differ only
    in the letter tag (a to h). The difference is divided exactly as in pooled(): by the mean of the
    two jobs over the scenario's runs, with the same floor. `rel_nofloor` keeps the figure without
    the floor, for the record."""
    rows = []
    for (arm, sc, rep), d in df.groupby(["arm", "scenario", "rep"]):
        for col, _ in PRIMARY:
            level = df[(df.arm == arm) & (df.scenario == sc)]
            jobs = sorted(d.job)
            for i, a in enumerate(jobs):
                for b in jobs[i + 1:]:
                    strip = lambda j: "+".join(o for o in j.split("+") if o not in list("abcdefgh"))  # noqa: E731
                    plugin = lambda j: "noplugin" not in j  # noqa: E731
                    if strip(a).replace("+noplugin", "") != strip(b).replace("+noplugin", ""):
                        continue
                    mean = float(level[level.job.isin([a, b])][col].mean())
                    scale = max(mean, POOL_FLOOR.get(col, 0.0))
                    if not scale > 0:
                        continue
                    diff = float(d[d.job == a][col].iloc[0] - d[d.job == b][col].iloc[0])
                    rows.append({"arm": arm, "scenario": sc, "rep": rep, "col": col, "a": a, "b": b,
                                 "diff": diff, "rel": diff / scale,
                                 "rel_nofloor": diff / mean if mean > 0 else np.nan,
                                 "same_plugin": plugin(a) == plugin(b)})
    return pd.DataFrame(rows)


def noise_margin(col, root=None):
    """The equivalence margin fixed in the analysis plan: the mean absolute relative difference
    between two identical jobs in one run, from the noise-floor campaign's runs.csv. None if that
    campaign has not been analysed yet."""
    path = os.path.join(root or os.path.join(ROOT, EXPERIMENTS["e2"]["root"]), "pairs.csv")
    if not os.path.exists(path):
        return None
    pairs = pd.read_csv(path)
    pairs = pairs[(pairs.col == col) & pairs.same_plugin & pairs.arm.str.startswith("aa-")]
    return float(pairs.rel.abs().mean()) if len(pairs) else None


def noise_numbers(df, root, numbers, tag):
    """The noise floor: how much identical jobs differ side by side, with and without the observe-only
    plugin, and how a job alone compares with the same job among three others."""
    pairs = identical_pairs(df[df.arm.str.startswith("aa-")])
    if not len(pairs):
        return ""
    pairs.to_csv(os.path.join(root, "pairs.csv"), index=False)
    out = ["=== noise floor: identical jobs side by side (relative difference = difference / mean of the two jobs "
           "in the scenario, with the floor of the pooled contrast)"]
    short = {"slo_violation_s": "slo", "slots_mean": "slots"}
    for col, label in PRIMARY:
        p = pairs[pairs.col == col]
        same = p[p.same_plugin]
        margin = same.rel.abs().mean()
        numbers.add(macro_name(tag, "noise", short[col], "margin"), f"{100 * margin:.0f}")
        numbers.add(macro_name(tag, "noise", short[col], "p95"), f"{100 * same.rel.abs().quantile(0.95):.0f}")
        numbers.add(macro_name(tag, "noise", short[col], "pairs"), str(len(same)))
        out.append(f"  {col:18s} identical pairs n={len(same)}: mean |rel diff| {100 * margin:.1f}%, "
                   f"95th percentile {100 * same.rel.abs().quantile(0.95):.1f}% "
                   f"(without the floor: {100 * same.rel_nofloor.abs().mean():.1f}%)")
        for arm, d in same.groupby("arm"):
            numbers.add(macro_name(tag, "noise", arm, short[col], "margin"), f"{100 * d.rel.abs().mean():.0f}")
            out.append(f"    {arm}: n={len(d)}, mean |rel diff| {100 * d.rel.abs().mean():.1f}%")
        # With the run as the unit: the mean absolute difference of a run's pairs, then the mean over
        # runs. A run with four identical jobs has six pairs from four values, so pairs are not
        # independent and must not be counted as such.
        per_run = same.groupby(["arm", "scenario", "rep"]).rel.apply(lambda x: float(x.abs().mean()))
        m, lo, hi = boot_ci(per_run.to_numpy(float))
        for part, v in (("run margin", m), ("run margin lo", lo), ("run margin hi", hi)):
            numbers.add(macro_name(tag, "noise", short[col], part), f"{100 * v:.1f}")
        numbers.add(macro_name(tag, "noise", short[col], "runs"), str(len(per_run)))
        # With few runs the interval of a mean of absolute values can reach below zero; the range of
        # the runs and the mean without the largest run say the same thing in plain numbers.
        numbers.add(macro_name(tag, "noise", short[col], "run margin min"), f"{100 * per_run.min():.1f}")
        numbers.add(macro_name(tag, "noise", short[col], "run margin max"), f"{100 * per_run.max():.1f}")
        if len(per_run) > 2:
            numbers.add(macro_name(tag, "noise", short[col], "run margin rest"),
                        f"{100 * per_run.sort_values().iloc[:-1].mean():.1f}")
        out.append(f"    with the run as the unit: {100 * m:.1f}% [{100 * lo:.1f}, {100 * hi:.1f}] over {len(per_run)} runs")
        out.append(f"    by run: {', '.join(f'{100 * v:.1f}%' for v in sorted(per_run))}")
        # The statistic of the model contrasts for two identical jobs: one pair per run, signed.
        one = same.sort_values(["a", "b"]).groupby(["arm", "scenario", "rep"]).first().rel.to_numpy(float)
        m, lo, hi = boot_ci(one)
        for part, v in (("mean", m), ("lo", lo), ("hi", hi)):
            numbers.add(macro_name(tag, "aa", short[col], part), f"{100 * v:+.1f}".replace("+", "$+$").replace("-", "$-$"))
        numbers.add(macro_name(tag, "aa", short[col], "sd"), f"{100 * one.std(ddof=1):.0f}")
        out.append(f"    two identical jobs, one pair per run: {100 * m:+.1f}% [{100 * lo:+.1f}, {100 * hi:+.1f}], "
                   f"SD {100 * one.std(ddof=1):.1f}% over {len(one)} runs")
        for sc, d in same.groupby("scenario"):
            numbers.add(macro_name(tag, "noise", sc, short[col], "abs"),
                        whole(float(d["diff"].abs().mean())) if col == "slo_violation_s" else num(float(d["diff"].abs().mean())))
            numbers.add(macro_name(tag, "noise", sc, short[col], "margin"), f"{100 * d.rel.abs().mean():.0f}")
            out.append(f"    {sc}: n={len(d)}, mean |diff| {d['diff'].abs().mean():.2f} in the metric's unit, "
                       f"mean |rel diff| {100 * d.rel.abs().mean():.1f}%")
        # Does the observe-only plugin change a built-in job? Pairs with against without the plugin.
        # The unit is the run: the mean of its jobs with the plugin minus the mean of those without.
        effect = []
        for (arm, sc, rep), d in df[df.arm.str.startswith("aa-")].groupby(["arm", "scenario", "rep"]):
            without = d[d.job.str.contains("noplugin")][col]
            with_plugin = d[~d.job.str.contains("noplugin")][col]
            scale = max(df[(df.arm == arm) & (df.scenario == sc)][col].mean(), POOL_FLOOR.get(col, 0.0))
            if len(without) and len(with_plugin) and scale > 0:
                effect.append((with_plugin.mean() - without.mean()) / scale)
        if len(effect) > 1:
            mean, lo, hi = boot_ci(np.array(effect))
            for part, v in (("mean", mean), ("lo", lo), ("hi", hi)):
                numbers.add(macro_name(tag, "plugin effect", short[col], part), f"{100 * v:+.0f}".replace("+", "$+$").replace("-", "$-$"))
            numbers.add(macro_name(tag, "plugin effect", short[col], "runs"), str(len(effect)))
            out.append(f"    with the observe-only plugin minus without: {100 * mean:+.1f}% [{100 * lo:+.1f}, {100 * hi:+.1f}] (n={len(effect)} runs)")
    # A job alone on the cluster against the same job among three others (different runs, unpaired).
    for solo, among in (("solo-builtin", "aa-builtin"), ("solo-rule", "aa-rule")):
        a, b = df[(df.arm == solo)], df[(df.arm == among) & ~df.job.str.contains("noplugin")]
        for sc in sorted(set(a.scenario) & set(b.scenario)):
            for col, _ in PRIMARY:
                x, y = a[a.scenario == sc][col].to_numpy(float), b[b.scenario == sc][col].to_numpy(float)
                if len(x) and len(y):
                    numbers.add(macro_name(tag, solo, sc, short[col]), num(float(x.mean())))
                    numbers.add(macro_name(tag, among, sc, short[col]), num(float(y.mean())))
                    out.append(f"  {sc} {col}: alone {x.mean():.1f} (n={len(x)}), among four {y.mean():.1f} (n={len(y)})")
    return "\n".join(out)


def equivalence_numbers(df, numbers, tag):
    """For each model against the policy in code: is the interval of the pooled difference within
    the noise margin (the criterion fixed before the campaign)?"""
    short = {"slo_violation_s": "slo", "slots_mean": "slots"}
    out = []
    rule = next((j for j in JOB_ORDER if j.split("+")[0] == "rule"), None)
    for col, _ in PRIMARY:
        margin = noise_margin(col)
        if margin is None or rule is None:
            continue
        numbers.add(macro_name(tag, "margin", short[col]), f"{100 * margin:.0f}")
        for arm in [a for a in ARMS if (df.arm == a).any()]:
            for job in [j for j in JOB_ORDER if analyze.has_model(j)]:
                r = next((t[3] for t in pooled_tests(df, arm) if t[:3] == (job, rule, col)), None)
                if r:
                    inside = -margin <= r["lo"] and r["hi"] <= margin
                    numbers.add(macro_name(tag, "equivalent", arm, NAME[job], short[col]), "yes" if inside else "no")
                    # One decimal for the text: an interval that ends 0.2 points inside the margin must not print as the margin.
                    for part in ("lo", "hi"):
                        numbers.add(macro_name(tag, "equivalent", arm, NAME[job], short[col], part),
                                    f"{100 * r[part]:+.1f}".replace("+", "$+$").replace("-", "$-$"))
                    out.append(f"  {arm} {NAME[job]} - {NAME[rule]} {col}: [{100 * r['lo']:+.0f}%, {100 * r['hi']:+.0f}%] "
                               f"against a margin of {100 * margin:.0f}%: {'within' if inside else 'NOT within'}")
    return ("=== equivalence with the policy in code (interval within the noise margin)\n" + "\n".join(out)) if out else ""


def holm(pvalues):
    """Holm-adjusted p-values, in the order given."""
    order = np.argsort(pvalues)
    adjusted, running = np.empty(len(pvalues)), 0.0
    for rank, i in enumerate(order):
        running = max(running, (len(pvalues) - rank) * pvalues[i])
        adjusted[i] = min(1.0, running)
    return adjusted


def contrasts():
    """The comparisons that matter in a campaign: the pairs it names, or else every job against the
    reference job (the first unless the campaign names another) and each model against the policy
    in code when both are present."""
    if PAIRS:
        return list(PAIRS)
    first = REFERENCE or JOB_ORDER[0]
    out = [(j, first) for j in JOB_ORDER if j != first]
    rule = next((j for j in JOB_ORDER if j.split("+")[0] == "rule"), None)
    if rule and rule != first:
        out += [(j, rule) for j in JOB_ORDER if analyze.has_model(j)]
    return out


def pooled_tests(df, arm):
    """The pooled contrasts of an arm as (a, b, column, result, adjusted p). Holm's correction covers
    the primary family fixed in the analysis plan and nothing else: the adjusted p of every other
    contrast is None, and it is reported with its interval only. Computed once per arm, so that the
    macros, the digest and the equivalence check quote the same resampling."""
    if not POOL:
        return []
    if arm not in POOLED:
        cols = list(dict.fromkeys(PRIMARY_COLS + [c for c, _ in PRIMARY]))
        tests = [(a, b, col, pooled(df, arm, a, b, col)) for a, b in contrasts() for col in cols if col in df]
        tests = [t for t in tests if t[3]]
        family = [i for i, (a, b, col, _) in enumerate(tests) if (a, b) in PRIMARY_PAIRS and col in PRIMARY_COLS]
        adjusted = dict(zip(family, holm([tests[i][3]["p"] for i in family]))) if family else {}
        POOLED[arm] = [(a, b, col, r, adjusted.get(i)) for i, (a, b, col, r) in enumerate(tests)]
    return POOLED[arm]


def unpaired(df, scenario, job, col):
    """Difference of means between the two arms of a campaign, second minus first, with Welch's t
    interval: the runs of two arms are separate runs and are not paired."""
    if len(ARMS) != 2:
        return None
    x = df[(df.arm == ARMS[1]) & (df.scenario == scenario) & (df.job == job)][col].dropna().to_numpy(float)
    y = df[(df.arm == ARMS[0]) & (df.scenario == scenario) & (df.job == job)][col].dropna().to_numpy(float)
    if len(x) < 2 or len(y) < 2:
        return None
    if np.var(x) + np.var(y) == 0:
        return {"mean": float(x.mean() - y.mean()), "lo": float(x.mean() - y.mean()), "hi": float(x.mean() - y.mean()),
                "n": (len(x), len(y))}
    from scipy import stats
    ci = stats.ttest_ind(x, y, equal_var=False).confidence_interval(0.95)
    return {"mean": float(x.mean() - y.mean()), "lo": float(ci.low), "hi": float(ci.high), "n": (len(x), len(y))}


# ------------------------------------------------------------------ formatting

def num(v, digits=None):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "--"
    if digits is not None:
        return f"{v:,.{digits}f}"
    a = abs(v)
    return f"{v:,.0f}" if a >= 100 else f"{v:.1f}" if a >= 10 else f"{v:.2f}" if a < 1 else f"{v:.1f}"


def whole(v):
    """Whole seconds for the text: the time over the limit is counted in seconds, and two identical
    jobs differ by tens of them."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "--"
    from decimal import ROUND_HALF_UP, Decimal
    out = f"{int(Decimal(repr(float(v))).quantize(Decimal(1), rounding=ROUND_HALF_UP)):,}"   # halves away from zero
    return out


def half_up(v, digits=1):
    """A mean with halves rounded up (2.25 -> 2.3); Python's format rounds them to even (2.2)."""
    from decimal import ROUND_HALF_UP, Decimal
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "--"
    return str(Decimal(repr(float(v))).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP))


def macro_name(*parts):
    """LaTeX macro names may only hold letters, so digits and separators are spelled out."""
    words = {"0": "Zero", "1": "One", "2": "Two", "3": "Three", "4": "Four", "5": "Five", "6": "Six",
             "7": "Seven", "8": "Eight", "9": "Nine"}
    out = ""
    for p in parts:
        for token in re.split(r"[^A-Za-z0-9]+", str(p)):
            out += "".join(words.get(ch, ch) for ch in token.capitalize())
    return out


class Numbers:
    def __init__(self):
        self.lines = []

    def add(self, name, value):
        self.lines.append(f"\\newcommand{{\\{name}}}{{{value}}}")

    def write(self, path):
        open(path, "w").write("% Generated by bench/paper.py. Do not edit.\n" + "\n".join(sorted(set(self.lines))) + "\n")


# ------------------------------------------------------------------ tables

MAIN_COLS = [("slo_violation_s", "Over limit (s)"), ("latency_p95_s", "p95 lat. (s)"),
             ("slots_mean", "Slots"), ("scaling_actions", "Act.")]


def table_main(df, path, numbers, tag="main"):
    arms = [a for a in ARMS if (df.arm == a).any()]
    lines = ["\\begin{tabular}{@{}ll" + ("rrrr" * len(arms)) + "@{}}", "\\toprule"]
    head = " & " + "".join(f" & \\multicolumn{{4}}{{c}}{{{ARM_NAME[a].capitalize()}}}" for a in arms) + " \\\\"
    lines.append(head)
    lines.append("".join(f"\\cmidrule(lr){{{3 + 4 * i}-{6 + 4 * i}}}" for i in range(len(arms))))
    lines.append("Scenario & Controller" + "".join(f" & {t}" for _ in arms for _, t in MAIN_COLS) + " \\\\")
    lines.append("\\midrule")
    for sc in present(df):
        jobs = [j for j in JOB_ORDER if ((df.scenario == sc) & (df.job == j)).any()]
        for k, job in enumerate(jobs):
            cells = []
            for arm in arms:
                d = df[(df.arm == arm) & (df.scenario == sc)]
                for col, _ in MAIN_COLS:
                    x = d[d.job == job][col]
                    mean, lo, hi = boot_ci(x)
                    numbers.add(macro_name(tag, arm, sc, NAME[job], col.split("_")[0] + col.split("_")[-1]), num(mean))
                    txt = num(mean)
                    if col in ("latency_p95_s", "slo_violation_s") and len(x) > 1:
                        txt += f" {{\\tiny [{num(lo)}, {num(hi)}]}}"
                    cells.append(txt)
            n = df[(df.scenario == sc)].groupby("arm").rep.nunique().to_dict()
            first = f"\\multirow{{{len(jobs)}}}{{*}}{{\\shortstack[l]{{{sc.replace('_', '-')}\\\\{{\\tiny $n$=" \
                    + "/".join(str(n.get(a, 0)) for a in arms) + "}}}" if k == 0 else ""
            lines.append(f"{first} & {NAME[job]} & " + " & ".join(cells) + " \\\\")
        lines.append("\\addlinespace")
    lines[-1] = "\\bottomrule"
    lines.append("\\end{tabular}")
    open(path, "w").write("\n".join(lines) + "\n")


def table_signal(df, path, numbers, tag="main"):
    lines = ["\\begin{tabular}{@{}llrr@{}}", "\\toprule",
             "Backlog signal & Controller & Backlog seen & Demand seen \\\\", "\\midrule"]
    for arm in [a for a in ARMS if (df.arm == a).any()]:
        d = df[(df.arm == arm) & (df.overload_cycles > 0)] if "overload_cycles" in df else df.iloc[0:0]
        for k, job in enumerate([j for j in JOB_ORDER if (d.job == j).any()]):
            b, lo, hi = boot_ci(d[d.job == job].backlog_seen_ratio)
            q, qlo, qhi = boot_ci(d[d.job == job].demand_seen_ratio)
            numbers.add(macro_name(tag, "seen", arm, NAME[job], "runs"), str(int(d[d.job == job].backlog_seen_ratio.notna().sum())))
            numbers.add(macro_name(tag, "seen", arm, NAME[job], "scenarios"), ", ".join(sorted(set(d[d.job == job].scenario))))
            numbers.add(macro_name(tag, "seen", arm, NAME[job], "backlog"), f"{100 * b:.0f}")
            numbers.add(macro_name(tag, "seen", arm, NAME[job], "demand"), f"{100 * q:.0f}")
            lines.append((ARM_NAME[arm].split()[0].capitalize() if k == 0 else "") +
                         f" & {NAME[job]} & {100 * b:.0f}\\% & {100 * q:.0f}\\% \\\\")
        lines.append("\\addlinespace")
    lines[-1] = "\\bottomrule"
    lines.append("\\end{tabular}")
    open(path, "w").write("\n".join(lines) + "\n")


def table_models(df, path, numbers, tag="main"):
    lines = ["\\begin{tabular}{@{}lrrrrr@{}}", "\\toprule",
             "Controller & Calls/run & Latency (s) & Slowest (s) & \\$/job-hour & Fallbacks \\\\", "\\midrule"]
    for job in [j for j in JOB_ORDER if analyze.has_model(j) and (df.job == j).any()]:
        d = df[df.job == job]
        vals = (d.model_calls.mean(), d.model_latency_ms_mean.mean() / 1000, d.model_latency_ms_max.max() / 1000,
                d.model_cost_usd_per_hour.mean(), int(d.model_fallbacks.sum()))
        for key, v in zip(("calls", "latency", "slowest", "cost", "fallbacks"), vals):
            numbers.add(macro_name(tag, "model", NAME[job], key), num(v) if key != "cost" else f"{v:.3f}")
        numbers.add(macro_name(tag, "model", NAME[job], "totalcalls"), f"{int(d.model_calls.sum()):,}")
        lines.append(f"{NAME[job]} & {vals[0]:.1f} & {vals[1]:.2f} & {vals[2]:.1f} & {vals[3]:.3f} & {vals[4]} \\\\")
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


def table_offline(path, numbers):
    p = os.path.join(ROOT, "results", "model_eval_random", "summary.json")
    if not os.path.exists(p):
        return
    s = json.load(open(p))
    label = {"jev-raw": "Decision model, bare options", "jev-computed": "Decision model, computed options",
             "haiku": "Haiku 5.5", "sonnet": "Sonnet 5.5", "sonnet-high": "Sonnet 5.5, high effort",
             "opus": "Opus 5.5", "fable": "Fable 5.2"}
    tokens = {}
    for v in label:
        f = os.path.join(ROOT, "results", "model_eval_random", f"{v}.jsonl")
        if os.path.exists(f):
            rs = [json.loads(line) for line in open(f)]
            tokens[v] = int(np.median([r["output_tokens"] for r in rs if r.get("answer") is not None]))
    lines = ["\\begin{tabular}{@{}lrrrrrr@{}}", "\\toprule",
             "Model & Exact & Low & High & Out.\\ tok. & Lat.\\ (s) & \\$/1k \\\\", "\\midrule"]
    for v, name in label.items():
        if v not in s:
            continue
        r = s[v]
        numbers.add(macro_name("off", v, "exact"), f"{100 * r['exact'] / r['n']:.0f}")
        numbers.add(macro_name("off", v, "n"), str(r["n"]))
        numbers.add(macro_name("off", v, "tokens"), str(tokens.get(v, 0)))
        tok = "--" if v.startswith("jev") else str(tokens.get(v, 0))
        lines.append(f"{name} & {100 * r['exact'] / r['n']:.0f}\\% & {r['low']} & {r['high']} & {tok} & "
                     f"{r['median_latency_ms'] / 1000:.2f} & {1000 * r['cost_per_decision']:.2f} \\\\")
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


# ------------------------------------------------------------------ figures

def gaps(t, y, longest=25.0):
    """(t, y) with a break wherever two autoscaler cycles are more than `longest` seconds apart: after
    a change the autoscaler has no cycle until its metric window is full again."""
    t, y = np.asarray(t, float), np.asarray(y, float)
    cut = np.flatnonzero(np.diff(t) > longest) + 1
    return np.insert(t, cut, np.nan), np.insert(y, cut, np.nan)


def fig_signal(runs, path, scenario="spike", job=None, jobs=None):
    """What the autoscaler believed against what was true. One column per arm for one job, or, with
    `jobs`, one column per job of the first arm; always the first repetition."""
    arms = [a for a in ARMS if any(k[0] == a and k[1] == scenario for k in runs)]
    if not arms:
        return
    cols = [(arms[0], j) for j in jobs] if jobs else [(a, job or JOB_ORDER[0]) for a in arms]
    fig, axes = plt.subplots(2, len(cols), figsize=(7.0 if len(cols) > 1 else 3.4, 3.0), sharex=True,
                             sharey="row", squeeze=False)
    for c, (arm, j) in enumerate(cols):
        rep = min(k[2] for k in runs if k[0] == arm and k[1] == scenario)
        meta, samples, latency, load, decisions = runs[(arm, scenario, rep)]
        s = samples[samples.job == j]
        if not len(s):
            continue
        b = beliefs(meta, samples, load, decisions, j)
        if "position_offset" in s and s.position_offset.notna().any():
            lag_t, lag = analyze.true_backlog(s, meta)
        else:
            # Runs saved before the readers' position was sampled (the pilots): end offsets minus
            # committed offsets, an upper bound that is up to one checkpoint interval too high.
            lag_t = s.t.to_numpy(float)
            lag = np.clip((s.end_offset - s.committed_offset.ffill()).to_numpy(float), 0, None)
        title = NAME[j] if jobs else ARM_NAME[arm].capitalize()
        ax = axes[0, c]
        ax.plot(load.t, load.target_rate, color=INK, label="arriving (true)")
        ax.plot(*gaps(b.t, b.believed_demand), color=COLOR[j], marker="o", ms=2.2, lw=1.0, label="demand the rule computed")
        ax.set_title(f"{title}: demand (records/s)", loc="left")
        ax.set_ylim(bottom=0)
        ax = axes[1, c]
        ax.plot(lag_t, lag / 1000, color=INK, label="true backlog")
        ax.plot(*gaps(b.t, b.believed_backlog / 1000), color=COLOR[j], marker="o", ms=2.2, lw=1.0, label="backlog the rule used")
        ax.set_title("Backlog (thousand records)", loc="left")
        ax.set_xlabel("Seconds")
        ax.set_xlim(0, meta["duration"])
        ax.set_ylim(bottom=0)
    axes[0, 0].legend(loc="upper left", handlelength=1.2, fontsize=6.3)
    axes[1, 0].legend(loc="upper left", handlelength=1.2, fontsize=6.3)
    fig.tight_layout(pad=0.4)
    fig.savefig(path)
    plt.close(fig)


def fig_surge(runs, df, path, pair, scenario="surge"):
    """One run of the sustained overload for two jobs that differ in the backlog signal: the demand
    and the backlog the rule believed (dots, one per autoscaler cycle) against the truth (lines), with
    every scale-up of Aggregate marked by its new parallelism. The run shown is the one whose time
    over the limit, for the job with the default signal, is closest to the median over runs."""
    default, fixed = pair
    d = df[(df.scenario == scenario) & (df.job == default)]
    if not len(d):
        return None
    rep = int(d.iloc[(d.slo_violation_s - d.slo_violation_s.median()).abs().argsort().iloc[0]].rep)
    key = next(k for k in runs if k[1] == scenario and k[2] == rep)
    meta, samples, latency, load, decisions = runs[key]
    label = {default: "default signal", fixed: "broker-side signal"}
    colour = {default: BLUE, fixed: AQUA}  # the same two colours whichever band is shown
    # The two signals also differ in line style and marker, so that a grey print still tells them apart.
    dash = {default: "-", fixed: (0, (4, 1.5))}
    mark = {default: "o", fixed: "s"}
    fig, axes = plt.subplots(2, 1, figsize=(3.4, 2.6), sharex=True)
    top, bottom = axes
    zero = (meta.get("events") or [0])[0]  # the figure counts from the start of the overload
    top.plot(load.t - zero, load.target_rate, color=INK, lw=1.0, label="arriving")
    for j in (default, fixed):
        s = samples[samples.job == j]
        b = beliefs(meta, samples, load, decisions, j)
        lag_t, lag = analyze.true_backlog(s, meta)
        top.plot(*gaps(b.t - zero, b.believed_demand), color=colour[j], marker=mark[j], ms=2.4, lw=0.8, ls=dash[j],
                 label=f"believed, {label[j]}")
        bottom.plot(np.asarray(lag_t, float) - zero, lag / 1000, color=colour[j], lw=1.3, ls=dash[j], label=f"true, {label[j]}")
        # Above the thin lines and not cut off at the axis: a believed backlog near zero is the point of the panel.
        # Marks left of the axis are dropped: without clipping they ran into the axis label.
        shown = b[(b.t - zero) >= -40]
        bottom.plot(*gaps(shown.t - zero, shown.believed_backlog / 1000), color=colour[j], marker=mark[j], ms=2.8, lw=0, zorder=5,
                    clip_on=False, label=f"believed, {label[j]}")
        ups = scale_ups(meta, samples, load, decisions, j)
        for n, (_, u) in enumerate(ups.iterrows()):
            # Marks above the plot: the upper row for the default signal, the lower for the broker's.
            y = 1.16 if j == default else 1.04
            top.annotate(f"{u.to:.0f}", xy=(u.t - zero, y), xycoords=("data", "axes fraction"), ha="center", va="bottom",
                         fontsize=7.2, color=INK, annotation_clip=False,
                         bbox=dict(boxstyle="round,pad=0.15", fc="white", ec=colour[j], lw=0.8))
    # The true backlog of the other runs as thin lines, so that the run shown can be judged against them.
    for other in sorted(k for k in runs if k[0] == key[0] and k[1] == scenario and k[2] != rep):
        ometa, osamples = runs[other][0], runs[other][1]
        ozero = (ometa.get("events") or [0])[0]
        for j in (default, fixed):
            ot, olag = analyze.true_backlog(osamples[osamples.job == j], ometa)
            bottom.plot(np.asarray(ot, float) - ozero, np.asarray(olag, float) / 1000, color=colour[j], lw=0.5, alpha=0.4, zorder=1,
                        ls=dash[j])
    top.set_ylabel("Records/s")
    top.set_ylim(bottom=0)
    top.set_title("Demand; boxes show each new size", loc="left", pad=22)
    bottom.set_ylabel("Thousand records")
    bottom.set_title("Backlog", loc="left")
    bottom.set_xlabel("Seconds since the overload began")
    bottom.set_xlim(-40, meta["duration"] - zero)
    # A little room below zero, so that dots on the zero line stand clear of the axis.
    bottom.set_ylim(bottom=-0.10 * bottom.get_ylim()[1])
    # One legend for both panels: colour is the signal, a line is the truth, a dot what the rule believed.
    from matplotlib.lines import Line2D
    handles = [Line2D([], [], color=colour[default], lw=1.3, ls=dash[default], marker=mark[default], ms=2.6, label=label[default]),
               Line2D([], [], color=colour[fixed], lw=1.3, ls=dash[fixed], marker=mark[fixed], ms=2.6, label=label[fixed]),
               Line2D([], [], color=INK, lw=1.0, label="true (lines)"),
               Line2D([], [], color=MUTED, lw=0, marker="o", ms=2.6, label="believed by the rule (marks)")]
    fig.legend(handles=handles, loc="lower center", ncol=2, handlelength=1.4, fontsize=6.3, columnspacing=1.2,
               borderaxespad=0.1, labelspacing=0.25)
    fig.tight_layout(pad=0.4, rect=(0, 0.09, 1, 1))
    fig.savefig(path)
    plt.close(fig)
    return rep


def table_sizing(df, path, numbers, tag):
    """Per job: what share of backlog and demand the rule saw, when it first scaled up and to what
    size next to what its formula gives for that cycle with true inputs, and the outcome. Pooled
    over scenarios and repetitions. All values also go to macros, with the scale-ups that were
    decided on a metric window wholly inside the changed workload ("late") separately."""
    lines = ["\\begin{tabular}{@{}lrrrrrrrr@{}}", "\\toprule",
             " & \\multicolumn{2}{c}{Seen} & \\multicolumn{3}{c}{First scale-up} & Scale- & To cap. & Over \\\\",
             "\\cmidrule(lr){2-3}\\cmidrule(lr){4-6}",
             " & backlog & demand & after & asked & Eq.~\\ref{eq:ds2} & ups & (s) & limit (s) \\\\", "\\midrule"]
    one = ("asked", "window", "trueinputs", "believed", "ups", "lateasked", "latewindow")
    for job in [j for j in JOB_ORDER if (df.job == j).any()]:
        d = df[df.job == job]
        o = d[d.overload_cycles > 0] if "overload_cycles" in d else d.iloc[0:0]
        u = d.dropna(subset=["first_up_to", "first_up_window"]) if "first_up_window" in d else d.iloc[0:0]
        late = d[d.late_ups > 0] if "late_ups" in d else d.iloc[0:0]
        mean = lambda frame, col: float(frame[col].mean()) if len(frame) and col in frame else np.nan  # noqa: E731
        total = lambda frame, col: float(frame[col].sum()) if len(frame) and col in frame else np.nan  # noqa: E731
        vals = {
            "backlog": 100 * mean(o, "backlog_seen_ratio"), "demand": 100 * mean(o, "demand_seen_ratio"),
            "firstup": mean(u, "first_up_s"), "asked": mean(u, "first_up_to"), "window": mean(u, "first_up_window"),
            "trueinputs": mean(u, "first_up_true"), "believed": mean(u, "first_up_believed"),
            "toosmall": 100 * float((u.first_up_to < u.first_up_window).mean()) if len(u) else np.nan,
            "ups": mean(d, "scale_ups"), "tocapacity": mean(d, "time_to_capacity_s"),
            "nocapacity": float(d.time_to_capacity_s.isna().sum()) if "time_to_capacity_s" in d else np.nan,
            "slo": mean(d, "slo_violation_s"), "lagmax": mean(d, "lag_max_records") / 1000,
            "upstoosmall": total(d, "ups_too_small"), "upschecked": total(d, "ups_checked"),
            "lateups": total(late, "late_ups"), "latetoosmall": total(late, "late_too_small"),
            "latedemand": 100 * mean(late, "late_demand_seen"), "latebacklog": 100 * mean(late, "late_backlog_seen"),
            "lateasked": mean(late, "late_asked"), "latewindow": mean(late, "late_window"),
        }
        for key, v in vals.items():
            numbers.add(macro_name(tag, "sizing", NAME[job], key), "--" if np.isnan(v) else f"{v:.1f}" if key in one else f"{v:.0f}")
        f = lambda v, fmt: "--" if np.isnan(v) else fmt.format(v)  # noqa: E731
        lines.append(f"{NAME[job]} & {f(vals['backlog'], '{:.0f}')}\\% & {f(vals['demand'], '{:.0f}')}\\% & "
                     f"{f(vals['firstup'], '{:.0f}')}\\,s & {f(vals['asked'], '{:.1f}')} & {f(vals['window'], '{:.1f}')} & "
                     f"{f(vals['ups'], '{:.1f}')} & {f(vals['tocapacity'], '{:.0f}')} & {f(vals['slo'], '{:.0f}')} \\\\")
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


def first_step_numbers(df, numbers, tag):
    """Per scenario and job: when the first scale-up of Aggregate came and what it asked for, the time
    until the running parallelism covered the load at nominal capacity, and the peak of the true
    backlog in thousands of records."""
    for (scenario, job), d in df.groupby(["scenario", "job"]):
        if job not in NAME:
            continue
        for key, col, digits in (("at", "first_up_s", 0), ("asked", "first_up_to", 1),
                                 ("capacity", "time_to_capacity_s", 0)):
            if col in d and d[col].notna().any():
                numbers.add(macro_name(tag, "first", scenario, NAME[job], key), num(float(d[col].mean()), digits))
        numbers.add(macro_name(tag, "first", scenario, NAME[job], "lagmax"), f"{d.lag_max_records.mean() / 1000:.0f}")
        numbers.add(macro_name(tag, "first", scenario, NAME[job], "runs"), str(len(d)))


def table_surge(df, ups, path, numbers, tag):
    """The sustained-overload campaign, one row per band and signal: the scale-ups of Aggregate, what
    the rule believed at the first scale-up it decided on a metric window wholly inside the overload,
    the size it asked for there next to Equation (1) with true inputs, and the outcome."""
    lines = ["\\begin{tabular}{@{}llrrrrrrr@{}}", "\\toprule",
             " & & Scale- & \\multicolumn{3}{c}{First full window} & Full & Over & Peak \\\\",
             "\\cmidrule(lr){4-6}",
             "Band & Signal & ups & dem. & backl. & asked/Eq.\\,\\ref{eq:ds2} & size (s) & limit (s) & backl. \\\\", "\\midrule"]
    previous = None
    for job in [j for j in JOB_ORDER if (df.job == j).any()]:
        d = df[df.job == job]
        u = ups[(ups.job == job) & (ups.full_window == True)] if len(ups) else ups  # noqa: E712
        u = u[u.true_rate_window > 0] if len(u) else u
        # Per run, the first scale-up that was decided on a metric window wholly inside the overload:
        # the rule has then seen nothing but the overload, and the job has been backpressured throughout.
        u = u.sort_values("t").groupby("rep").head(1) if len(u) else u
        mean = lambda series: float(series.mean()) if len(series) else np.nan  # noqa: E731
        every = ups[ups.job == job].sort_values("t") if len(ups) else ups
        first = every.groupby("rep").head(1) if len(every) else every
        # The scale-up after the one above, in the runs that had one: did the backlog grow in between?
        after = []
        for _, row in (u.iterrows() if len(u) else []):
            later = every[(every.rep == row.rep) & (every.t > row.t)]
            if len(later) and not np.isnan(later.iloc[0].get("true_backlog", np.nan)):
                after.append((row.true_backlog, later.iloc[0].true_backlog))
        after = np.array(after) if after else np.empty((0, 2))
        vals = {
            "ups": mean(d.scale_ups), "slo": mean(d.slo_violation_s), "lagmax": mean(d.lag_max_records) / 1000,
            "tocapacity": mean(d.time_to_capacity_s), "decisions": float(len(u)),
            "tofull": mean(d.time_to_full_s), "tofullmin": float(d.time_to_full_s.min()),
            "tofullmax": float(d.time_to_full_s.max()), "fullsize": mean(d.full_size),
            "nofull": float(d.time_to_full_s.isna().sum()),
            "twoups": float((d.scale_ups <= 2).sum()), "runs": float(len(d)),
            "truebacklog": mean(u.true_backlog) / 1000 if len(u) else np.nan,
            "backlogbefore": float(after[:, 0].mean()) / 1000 if len(after) else np.nan,
            "backlogafter": float(after[:, 1].mean()) / 1000 if len(after) else np.nan,
            "backloggrew": float((after[:, 1] > after[:, 0]).sum()) if len(after) else np.nan,
            "backlogpairs": float(len(after)),
            "secondat": mean(u.since_event) if len(u) else np.nan,
            "firstat": mean(first.since_event) if len(first) else np.nan,
            "firstatmin": float(first.since_event.min()) if len(first) else np.nan,
            "firstatmax": float(first.since_event.max()) if len(first) else np.nan,
            "firstasked": mean(first.to) if len(first) else np.nan,
            "firstaskedmin": float(first.to.min()) if len(first) else np.nan,
            "firstaskedmax": float(first.to.max()) if len(first) else np.nan,
            "firsteq": mean(first.size_window) if len(first) and "size_window" in first else np.nan,
            "slotsmean": mean(d.slots_mean), "latency": mean(d.latency_p95_s),
            "restarts": mean(d.scaling_actions),
            "censored": float(d.slo_censored.sum()) if "slo_censored" in d else np.nan,
            # Jobs that were under the limit again before the overload ended (they caught up).
            "caughtup": float((d.slo_last_s < d.overload_end_s).sum()) if "overload_end_s" in d else np.nan,
            "demandshort": mean(d.demand_ratio_short) if "demand_ratio_short" in d else np.nan,
            "demanddrain": mean(d.demand_ratio_drain) if "demand_ratio_drain" in d else np.nan,
            "needraw": mean(u.need_window) if len(u) and "need_window" in u else np.nan,
            "needbelieved": mean(u.need_believed) if len(u) and "need_believed" in u else np.nan,
            "needdemand": mean(u.need_demand) if len(u) and "need_demand" in u else np.nan,
            "needbacklog": mean(u.need_backlog) if len(u) and "need_backlog" in u else np.nan,
            "dneedbelieved": mean(u.dneed_believed) if len(u) and "dneed_believed" in u else np.nan,
            "dneeddemand": mean(u.dneed_demand) if len(u) and "dneed_demand" in u else np.nan,
            "dneedbacklog": mean(u.dneed_backlog) if len(u) and "dneed_backlog" in u else np.nan,
            "dneedboth": mean(u.dneed_both) if len(u) and "dneed_both" in u else np.nan,
            "backlogbound": 100 * mean(u.believed_backlog / u.bound_backlog.clip(lower=1)) if len(u) and "bound_backlog" in u else np.nan,
            "alldemand": 100 * mean(d.late_demand_seen) if "late_demand_seen" in d else np.nan,
            "allbacklog": 100 * mean(d.late_backlog_seen) if "late_backlog_seen" in d else np.nan,
            "slomin": float(d.slo_violation_s.min()), "slomax": float(d.slo_violation_s.max()),
            # What the job was able to process when that scale-up took effect, against the load.
            "askedcapacity": mean(u.to * u.believed_capacity) if len(u) and "believed_capacity" in u else np.nan,
            "demand": 100 * mean(u.believed_demand / u.true_rate_window) if len(u) else np.nan,
            "backlog": 100 * mean((u.believed_backlog / u.true_backlog)[u.true_backlog >= 10 * u.true_rate_window]) if len(u) else np.nan,
            "asked": mean(u.to) if len(u) else np.nan, "eq": mean(u.size_window) if len(u) else np.nan,
            "toosmall": float((u.to < u.size_window).sum()) if len(u) else np.nan,
            # The believed demand over the capacity of the job at that time: 1 means that the rule
            # took what the job was able to process for the demand.
            "overcapacity": mean(u.believed_demand / (u["from"] * u.believed_capacity))
            if len(u) and "believed_capacity" in u else np.nan,
            "growth": mean(u.to / u["from"]) if len(u) else np.nan,
        }
        # The most frequent sequence of sizes, for the text.
        paths = ups[ups.job == job].sort_values("t").groupby("rep").apply(
            lambda g: "$\\to$".join([f"{g['from'].iloc[0]:.0f}"] + [f"{v:.0f}" for v in g.to])) if len(ups) else pd.Series(dtype=str)
        if len(paths):
            numbers.add(macro_name(tag, "surge", NAME[job], "path"), paths.mode().iloc[0])
            numbers.add(macro_name(tag, "surge", NAME[job], "path runs"), str(int((paths == paths.mode().iloc[0]).sum())))
        for key, v in vals.items():
            numbers.add(macro_name(tag, "surge", NAME[job], key),
                        "--" if v is None or np.isnan(v) else f"{v:.2f}" if key in ("overcapacity", "growth")
                        else f"{v:.2f}" if key in ("demandshort", "demanddrain")
                        else f"{v:.1f}" if key in ("ups", "asked", "eq", "firstasked", "firsteq", "slotsmean", "restarts", "needraw",
                                                   "needbelieved", "needdemand", "needbacklog", "dneedbelieved", "dneeddemand",
                                                   "dneedbacklog", "dneedboth")
                        else f"{v:,.0f}")
        f = lambda v, fmt: "--" if np.isnan(v) else fmt.format(v)  # noqa: E731
        band, _, signal = NAME[job].partition(", ")
        signal = "broker" if signal else "default"
        shown = {"Stock": "Stock", "Matched": "Target"}.get(band.split()[0], band.split()[0])
        lines.append(f"{'' if band == previous else shown} & {signal} & {f(vals['ups'], '{:.1f}')} & "
                     f"{f(vals['demand'], '{:.0f}')}\\% & {f(vals['backlog'], '{:.0f}')}\\% & "
                     f"{f(vals['asked'], '{:.1f}')}/{f(vals['eq'], '{:.1f}')} & {f(vals['tofull'], '{:.0f}')} & "
                     f"{f(vals['slo'], '{:.0f}')} & {f(vals['lagmax'], '{:.0f}')}k \\\\")
        previous = band
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


# The closed-loop experiments on the backlog signal, in the order of the paper's table: campaign,
# arm, scenario, label, and per band the job with the broker-side signal and the job with the default.
CLOSED = [
    ("e8s", "default", "spike", "Spike, 2 min", [("stock", "builtin+lagfix", "builtin")]),
    ("e8", "default", "spike", "Spike, 2 min", [("target", "builtin+max70+lagfix", "builtin+max70")]),
    ("e8s", "default", "ramp", "Ramp over 4 min", [("stock", "builtin+lagfix", "builtin")]),
    ("e8", "default", "ramp", "Ramp over 4 min", [("target", "builtin+max70+lagfix", "builtin+max70")]),
    ("e8s", "default", "capacity", "Slow-down, 3 min", [("stock", "builtin+lagfix", "builtin")]),
    ("e8", "default", "capacity", "Slow-down, 3 min", [("target", "builtin+max70+lagfix", "builtin+max70")]),
    ("e8d", "default", "surge_low", "Overload to 600/s, 8 min",
     [("stock", "builtin+lagfix", "builtin"), ("target", "builtin+max70+lagfix", "builtin+max70")]),
    ("e8d", "default", "surge_mid", "Overload to 1,200/s, 8 min",
     [("stock", "builtin+lagfix", "builtin"), ("target", "builtin+max70+lagfix", "builtin+max70")]),
    ("e8b", "default", "surge", "Overload to 2,400/s, 8 min",
     [("stock", "builtin+lagfix", "builtin"), ("target", "builtin+max70+lagfix", "builtin+max70")]),
    ("e8c", "long", "surge_long", "To 2,400/s, 40 min, 5-min windows",
     [("stock", "builtin+lagfix", "builtin"), ("target", "builtin+max70+lagfix", "builtin+max70")]),
]
# The tests the analysis plan names, per campaign and scenario, as (band, column); Holm's correction
# is over the tests of one campaign. E8's two tests are pooled over its scenarios and reported in the
# text; the smallest overload of E8d (four runs) is reported without tests.
# Runs of E8c in which a task failed inside the overload (bench/job_exceptions.py, ISSUES.md #86), per
# execution. The first execution is in results-e8c-first/, the repeat in results-e8c/.
E8C_TASK_FAILURES = {"first": (2,), "second": ()}
CLOSED_PRIMARY = {
    ("e8b", "surge"): [("stock", "slo_violation_s"), ("stock", "scale_ups"), ("target", "slo_violation_s"), ("target", "scale_ups")],
    ("e8d", "surge_mid"): [("stock", "slo_violation_s"), ("stock", "scale_ups"), ("target", "slo_violation_s"), ("target", "scale_ups")],
    ("e8c", "surge_long"): [("target", "slo_violation_s")],
}


def table_closed(path, numbers):
    """One row per scenario and band: the job with the default signal next to the job with the
    broker-side signal, from the runs.csv that the analysis of each campaign wrote. Seconds over the
    limit with the paired difference, its interval and the corrected p of the plan's test; scale-ups
    of Aggregate; restarts on any vertex; mean slots; time until Aggregate ran at its full size."""
    lines = ["\\begin{tabular}{@{}llr rr r@{\\,}l r rr rr rr rr@{}}", "\\toprule",
             " & & & \\multicolumn{5}{c}{Seconds over the latency limit} & \\multicolumn{2}{c}{Scale-ups} & "
             "\\multicolumn{2}{c}{Restarts} & \\multicolumn{2}{c}{Slots} & \\multicolumn{2}{c}{Covers load (s)} \\\\",
             "\\cmidrule(lr){4-8}\\cmidrule(lr){9-10}\\cmidrule(lr){11-12}\\cmidrule(lr){13-14}\\cmidrule(lr){15-16}",
             "Scenario & Band & $n$ & def. & broker & \\multicolumn{2}{c}{difference [95\\% interval]} & $p$ & "
             "def. & broker & def. & broker & def. & broker & def. & broker \\\\", "\\midrule"]
    frames, pvalues = {}, {}
    for exp in dict.fromkeys(c[0] for c in CLOSED):
        f = os.path.join(ROOT, EXPERIMENTS[exp]["root"], "runs.csv")
        if os.path.exists(f):
            frames[exp] = pd.read_csv(f)
    rows, previous = [], None
    for exp, arm, scenario, label, bands in CLOSED:
        if exp not in frames:
            continue
        df = frames[exp]
        df = df[(df.arm == arm) & (df.scenario == scenario)]
        for band, fixed, default in bands:
            wide = {c: df.pivot(index="rep", columns="job", values=c) for c in
                    ("slo_violation_s", "scale_ups", "scaling_actions", "slots_mean", "time_to_full_s", "time_to_capacity_s",
                     "lag_max_records", "jm_restarts")
                    if c in df}
            slo = wide.get("slo_violation_s")
            if slo is None or fixed not in slo or default not in slo:
                continue
            both = slo[[fixed, default]].dropna()
            if len(both) < 2:
                continue
            rows.append((exp, scenario, label, band, fixed, default, wide, both.index))
            for col in ("slo_violation_s", "scale_ups"):
                d = (wide[col][fixed] - wide[col][default]).loc[both.index].to_numpy(float)
                pvalues[(exp, scenario, band, col)] = sign_flip_p(d)
    adjusted = {}
    for (exp, scenario), family in CLOSED_PRIMARY.items():
        keys = [k for k in pvalues if k[0] == exp and k[1] == scenario and (k[2], k[3]) in family]
        if keys:
            adjusted.update(dict(zip(keys, holm([pvalues[k] for k in keys]))))
    for exp, scenario, label, band, fixed, default, wide, reps in rows:
        def pair(col, fmt):
            if col not in wide or fixed not in wide[col] or default not in wide[col]:
                return "--", "--", np.nan, np.nan
            a, b = wide[col][default].loc[reps], wide[col][fixed].loc[reps]
            show = (lambda v: half_up(v, 1)) if fmt == "{:.1f}" else fmt.format  # means of counts: halves up
            return (show(a.mean()) if a.notna().any() else "--", show(b.mean()) if b.notna().any() else "--",
                    float(a.mean()), float(b.mean()))

        def reach(col):
            """Mean seconds until a level was reached, or 'k of n' where not every job got there, and
            the paired difference over the runs in which both did."""
            d_txt, b_txt, _, _ = pair(col, "{:.0f}")
            if col not in wide or fixed not in wide[col] or default not in wide[col]:
                return d_txt, b_txt, (np.nan, np.nan, np.nan)
            reached = [int(wide[col][j].loc[reps].notna().sum()) for j in (default, fixed)]
            if min(reached) < len(reps):
                d_txt, b_txt = (f"{k} of {len(reps)}" for k in reached)
            both_reached = (wide[col][fixed] - wide[col][default]).loc[reps].dropna().to_numpy(float)
            return d_txt, b_txt, (boot_ci(both_reached) if len(both_reached) else (np.nan, np.nan, np.nan))
        d = (wide["slo_violation_s"][fixed] - wide["slo_violation_s"][default]).loc[reps].to_numpy(float)
        mean, lo, hi = boot_ci(d)
        p = adjusted.get((exp, scenario, band, "slo_violation_s"))
        slo_d, slo_b, _, _ = pair("slo_violation_s", "{:.0f}")
        ups_d, ups_b, _, _ = pair("scale_ups", "{:.1f}")
        act_d, act_b, _, _ = pair("scaling_actions", "{:.1f}")
        slot_d, slot_b, _, _ = pair("slots_mean", "{:.1f}")
        full_d, full_b, _ = reach("time_to_full_s")
        cap_d, cap_b, (cap_mean, cap_lo, cap_hi) = reach("time_to_capacity_s")
        _, _, slo_default_mean, _ = pair("slo_violation_s", "{:.0f}")
        ups = (wide["scale_ups"][fixed] - wide["scale_ups"][default]).loc[reps].to_numpy(float)
        ups_mean, ups_lo, ups_hi = boot_ci(ups)
        p_ups = adjusted.get((exp, scenario, band, "scale_ups"))
        full = (wide["time_to_full_s"][fixed] - wide["time_to_full_s"][default]).loc[reps].dropna().to_numpy(float) \
            if "time_to_full_s" in wide else np.array([])
        full_mean, full_lo, full_hi = boot_ci(full) if len(full) else (np.nan, np.nan, np.nan)
        key = (exp, scenario, band)
        for name, value in (
                ("n", str(len(reps))), ("slo default", slo_d), ("slo broker", slo_b),
                ("diff", f"{mean:.0f}"), ("diff lo", f"{lo:.0f}"), ("diff hi", f"{hi:.0f}"),
                ("lower", str(int((d < 0).sum()))), ("p", "--" if p is None else f"{p:.3f}"),
                ("p raw", f"{pvalues[(exp, scenario, band, 'slo_violation_s')]:.3f}"),
                ("ups default", ups_d), ("ups broker", ups_b), ("ups diff", f"{ups_mean:.2f}"),
                ("ups lo", f"{ups_lo:.2f}"), ("ups hi", f"{ups_hi:.2f}"), ("ups p", "--" if p_ups is None else f"{p_ups:.3f}"),
                ("restarts default", act_d), ("restarts broker", act_b), ("slots default", slot_d), ("slots broker", slot_b),
                ("full default", full_d), ("full broker", full_b), ("full diff", num(full_mean, 0)),
                ("full lo", num(full_lo, 0)), ("full hi", num(full_hi, 0)),
                # The plan's metric: seconds until the parallelism covers the peak load at nominal capacity.
                ("covers default", cap_d), ("covers broker", cap_b), ("covers diff", num(cap_mean, 0)),
                ("covers lo", num(cap_lo, 0)), ("covers hi", num(cap_hi, 0)),
                # The difference as a share of the time the default-signal job spent over the limit.
                ("share", f"{100 * abs(mean) / slo_default_mean:.0f}" if slo_default_mean > 0 else "--"),
                ("ups p raw", f"{pvalues[(exp, scenario, band, 'scale_ups')]:.3f}")):
            numbers.add(macro_name("closed", *key, name), value)
        first = label if label != previous else ""
        whole = lambda v: f"{v:.0f}".replace("-0", "0") if abs(v) < 0.5 else f"{v:.0f}"
        lines.append(f"{first} & {band} & {len(reps)} & {slo_d} & {slo_b} & ${whole(mean)}$ & [${whole(lo)}$, ${whole(hi)}$] & "
                     f"{'--' if p is None else f'{p:.3f}'} & {ups_d} & {ups_b} & {act_d} & {act_b} & "
                     f"{slot_d} & {slot_b} & {cap_d} & {cap_b} \\\\")
        previous = label
    # The band at the target against the shipped band, within runs, for each signal (not a planned
    # contrast; the bands campaign is the planned test).
    by = {}
    for exp, scenario, label, band, fixed, default, wide, reps in rows:
        by.setdefault((exp, scenario), {})[band] = (fixed, default, wide, reps)
    for (exp, scenario), b in by.items():
        if "stock" not in b or "target" not in b:
            continue
        wide, reps = b["stock"][2], b["stock"][3].intersection(b["target"][3])
        for signal, k in (("default", 1), ("broker", 0)):
            for col, key, digits in (("slo_violation_s", "slo", 0), ("slots_mean", "slots", 1), ("scaling_actions", "restarts", 2)):
                if col not in wide:
                    continue
                d = (wide[col][b["target"][k]] - wide[col][b["stock"][k]]).loc[reps].to_numpy(float)
                m, lo, hi = boot_ci(d)
                for part, v in (("diff", m), ("lo", lo), ("hi", hi)):
                    numbers.add(macro_name("bands", exp, scenario, signal, key, part), f"{v:+.{digits}f}".replace("+", "$+$").replace("-", "$-$"))
                numbers.add(macro_name("bands", exp, scenario, signal, key, "worse"), str(int((d > 0).sum())))
                numbers.add(macro_name("bands", exp, scenario, signal, key, "n"), str(len(d)))
    # The first execution of the overload campaign (before ISSUES.md #61 was repaired) next to the
    # second: each alone is in the text; here both together, and both without the runs with a kill.
    first_path = os.path.join(ROOT, "results-e8b-jm1g", "runs.csv")
    if "e8b" in frames and os.path.exists(first_path):
        both = {"second": frames["e8b"], "first": pd.read_csv(first_path)}
        for band, fixed, default in [x for c in CLOSED if c[0] == "e8b" for x in c[4]]:
            diffs, clean = {}, {}
            for name, f in both.items():
                w = f[(f.scenario == "surge")].pivot(index="rep", columns="job", values="slo_violation_s")
                killed = f[(f.scenario == "surge") & f.job.isin([fixed, default])].groupby("rep").jm_restarts.sum() > 0
                diffs[name] = (w[fixed] - w[default]).dropna()
                clean[name] = diffs[name][~killed.reindex(diffs[name].index, fill_value=False)]
            for key, d in (("first", diffs["first"]), ("both", pd.concat(diffs.values())), ("clean", pd.concat(clean.values())),
                           ("first clean", clean["first"])):
                m, lo, hi = boot_ci(d.to_numpy(float))
                for part, v in (("diff", m), ("lo", lo), ("hi", hi)):
                    numbers.add(macro_name("closed e8b", band, key, part), f"{v:.0f}")
                numbers.add(macro_name("closed e8b", band, key, "n"), str(len(d)))
            numbers.add(macro_name("closed e8b", band, "first killed"), str(len(diffs["first"]) - len(clean["first"])))
    # The campaign with longer windows was run twice as well (ISSUES.md #86): the first execution, the
    # first without its run with a task failure, both together, and both without such runs.
    first_path = os.path.join(os.environ.get("E8C_FIRST", os.path.join(ROOT, "results-e8c-first")), "runs.csv")
    if "e8c" in frames and os.path.exists(first_path):
        both = {"second": frames["e8c"], "first": pd.read_csv(first_path)}
        for band, fixed, default in [x for c in CLOSED if c[0] == "e8c" for x in c[4]]:
            diffs, firsts = {}, {}
            for name, f in both.items():
                f = f[f.scenario == "surge_long"]
                w = f.pivot(index="rep", columns="job", values="slo_violation_s")
                diffs[name] = (w[fixed] - w[default]).dropna()
                at = f.pivot(index="rep", columns="job", values="first_action_s")
                firsts[name] = (at[default] - at[fixed]).dropna()
            clean = {name: d.drop(index=[r for r in E8C_TASK_FAILURES.get(name, ()) if r in d.index]) for name, d in diffs.items()}
            for key, d in (("first", diffs["first"]), ("first clean", clean["first"]), ("both", pd.concat(diffs.values())),
                           ("clean", pd.concat(clean.values()))):
                m, lo, hi = boot_ci(d.to_numpy(float))
                for part, v in (("diff", m), ("lo", lo), ("hi", hi)):
                    numbers.add(macro_name("closed e8c", band, key, part), f"{v:.0f}")
                numbers.add(macro_name("closed e8c", band, key, "n"), str(len(d)))
                numbers.add(macro_name("closed e8c", band, key, "lower"), str(int((d < 0).sum())))
            for name, d in firsts.items():  # how much later the default signal's first step came
                m, lo, hi = boot_ci(d.to_numpy(float))
                for part, v in (("delay", m), ("delay lo", lo), ("delay hi", hi)):
                    numbers.add(macro_name("closed e8c", band, name, part), f"{v:.0f}")
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")
    return "\n".join(lines)


def fig_timeline(runs, path, scenario="spike"):
    """Aggregate parallelism and p95 latency over time, median over repetitions, per arm."""
    arms = [a for a in ARMS if any(k[0] == a and k[1] == scenario for k in runs)]
    if not arms:
        return
    fig, axes = plt.subplots(3, len(arms), figsize=(3.4 * len(arms), 3.9), sharex=True, squeeze=False,
                             gridspec_kw={"height_ratios": [0.7, 1, 1.2]})
    for c, arm in enumerate(arms):
        mine = [v for k, v in sorted(runs.items()) if k[0] == arm and k[1] == scenario]
        dur = mine[0][0]["duration"]
        grid = np.arange(-30, dur + 1, 5.0)
        axes[0, c].plot(mine[0][3].t, mine[0][3].target_rate, color=INK)
        axes[0, c].set_title(f"{ARM_NAME[arm].capitalize()}: input rate (records/s)", loc="left")
        axes[0, c].set_ylim(bottom=0)
        for job in [j for j in JOB_ORDER if j in mine[0][0]["jobs"]]:
            par, lat = [], []
            for meta, samples, latency, load, _ in mine:
                s = samples[samples.job == job]
                par.append(np.interp(grid, s.t, s.p_aggregate.ffill().bfill()))
                sec = analyze.per_second(latency, job, dur).p95.ffill(limit=3)
                lat.append(np.interp(grid, sec.index.to_numpy(float), sec.fillna(sec.max()).to_numpy(float)))
            axes[1, c].step(grid, np.median(par, axis=0), where="post", color=COLOR[job], label=NAME[job])
            axes[2, c].plot(grid, np.clip(np.median(lat, axis=0), 1, None) / 1000, color=COLOR[job], label=NAME[job])
        axes[1, c].set_title("Aggregate parallelism (median over runs)", loc="left")
        axes[1, c].set_ylim(0, MAX_PAR + 0.5)
        axes[2, c].set_title("p95 latency (s, log scale; median over runs)", loc="left")
        axes[2, c].set_yscale("log")
        axes[2, c].axhline(analyze.SLO_MS / 1000, color=MUTED, lw=0.7, ls="--")
        axes[2, c].set_xlabel("Seconds since the measured phase began")
        axes[2, c].set_xlim(-30, dur)
    # One legend above the panels: inside them it would cover the lines.
    handles, labels = axes[1, 0].get_legend_handles_labels()
    fig.tight_layout(pad=0.4, rect=(0, 0, 1, 0.94))
    fig.legend(handles, labels, loc="upper center", ncol=len(labels), columnspacing=1.0, handlelength=1.2,
               bbox_to_anchor=(0.5, 1.0), borderaxespad=0.1)
    fig.savefig(path)
    plt.close(fig)


def fig_summary(df, path):
    """Mean and 95% interval per controller, scenario and arm for the four headline metrics."""
    panels = [("latency_p95_s", "p95 latency (s)", True), ("slo_violation_s", "Seconds over the 2 s limit", False),
              ("parallelism_total_mean", "Average total parallelism", False), ("scaling_actions", "Scaling actions", False)]
    scenarios = present(df)
    arms = [a for a in ARMS if (df.arm == a).any()]
    jobs = [j for j in JOB_ORDER if (df.job == j).any()]
    fig, axes = plt.subplots(1, len(panels), figsize=(7.0, 2.5), sharey=True)
    for ax, (col, title, log) in zip(axes, panels):
        for i, sc in enumerate(scenarios):
            for a, arm in enumerate(arms):
                for k, job in enumerate(jobs):
                    x = df[(df.arm == arm) & (df.scenario == sc) & (df.job == job)][col]
                    if not len(x):
                        continue
                    mean, lo, hi = boot_ci(x)
                    y = i + (a - (len(arms) - 1) / 2) * 0.42 + (k - (len(jobs) - 1) / 2) * 0.085
                    ax.plot([lo, hi], [y, y], color=COLOR[job], lw=0.9)
                    ax.plot(mean, y, marker=MARK[job], ms=3.6, color=COLOR[job], mew=0.8,
                            mfc=COLOR[job] if arm == "default" else "white",
                            label=NAME[job] if (i == 0 and a == 0 and ax is axes[0]) else None)
        ax.set_title(title, loc="left")
        ax.set_yticks(range(len(scenarios)))
        ax.set_yticklabels([s.replace("_", "-") for s in scenarios])
        ax.grid(axis="y", visible=False)
        for i in range(1, len(scenarios)):
            ax.axhline(i - 0.5, color=GRID, lw=0.5)
        if log:
            ax.set_xscale("log")
    axes[0].invert_yaxis()
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=len(jobs), bbox_to_anchor=(0.42, -0.02))
    fig.text(0.99, 0.01, "filled: default signal; hollow: fresh signal; bars: 95% interval", ha="right", color=MUTED, fontsize=6.5)
    fig.tight_layout(pad=0.4, rect=(0, 0.06, 1, 1))
    fig.savefig(path)
    plt.close(fig)


def fig_pareto(df, path):
    """Resource use against time over the latency limit: lower left is better."""
    scenarios = present(df)
    fig, axes = plt.subplots(1, len(scenarios), figsize=(7.0, 1.9), squeeze=False)
    for ax, sc in zip(axes[0], scenarios):
        for arm in [a for a in ARMS if (df.arm == a).any()]:
            for job in [j for j in JOB_ORDER if (df.job == j).any()]:
                d = df[(df.arm == arm) & (df.scenario == sc) & (df.job == job)]
                if not len(d):
                    continue
                x, xl, xh = boot_ci(d.parallelism_total_mean)
                y, yl, yh = boot_ci(d.slo_violation_s)
                ax.plot([xl, xh], [y, y], color=COLOR[job], lw=0.7)
                ax.plot([x, x], [yl, yh], color=COLOR[job], lw=0.7)
                ax.plot(x, y, marker=MARK[job], ms=4, color=COLOR[job], mew=0.8,
                        mfc=COLOR[job] if arm == "default" else "white",
                        label=NAME[job] if (arm == "default" and ax is axes[0, 0]) else None)
        ax.set_title(sc.replace("_", "-"), loc="left")
        ax.set_xlabel("Average total parallelism")
        ax.set_ylim(bottom=0)
    axes[0, 0].set_ylabel("Seconds over the 2 s limit")
    axes[0, 0].legend(loc="upper right", handletextpad=0.2)
    fig.tight_layout(pad=0.4)
    fig.savefig(path)
    plt.close(fig)


# ------------------------------------------------------------------ digest

def contrast_numbers(df, numbers, tag):
    """Macros for the paired differences the text quotes: per scenario in the metric's unit, and
    pooled over the scenarios of an arm as a percentage of the scenario mean with the adjusted p."""
    short = {"slo_violation_s": "slo", "slots_mean": "slots", "scale_ups": "ups", "lag_max_records": "lagmax"}
    for arm in [a for a in ARMS if (df.arm == a).any()]:
        for a, b in contrasts():
            for col, key in short.items():
                if col not in df:
                    continue
                for sc in present(df, df.arm == arm):
                    r = paired(df, arm, sc, a, b, col)
                    if r:
                        for part in ("mean", "lo", "hi"):
                            numbers.add(macro_name(tag, "diff", arm, sc, NAME[a], "vs", NAME[b], key, part),
                                        whole(r[part]) if key == "slo" else num(r[part]))
        for a, b, col, r, adj in pooled_tests(df, arm):
            base = (tag, "pooled", arm, NAME[a], "vs", NAME[b], short[col])
            for part in ("mean", "lo", "hi"):
                numbers.add(macro_name(*base, part), f"{100 * r[part]:+.0f}".replace("+", "$+$").replace("-", "$-$"))
            if adj is not None:
                numbers.add(macro_name(*base, "p"), f"{adj:.2f}" if adj >= 0.1 else f"{adj:.3f}" if adj >= 0.001 else "<0.001")
            numbers.add(macro_name(*base, "n"), str(r["n"]))
        numbers.add(macro_name(tag, "tests", arm), str(sum(1 for t in pooled_tests(df, arm) if t[4] is not None)))


def kill_sensitivity(df, numbers, tag):
    """The primary contrasts once more without the runs in which a JobManager was killed (ISSUES.md
    #61): pooled, and per scenario in the metric's unit. Empty if no run of the campaign had a kill."""
    if "jm_restarts" not in df or not (df.jm_restarts > 0).any():
        return ""
    short = {"slo_violation_s": "slo", "scale_ups": "ups", "slots_mean": "slots"}
    hit = set(map(tuple, df[df.jm_restarts > 0][["arm", "scenario", "rep"]].to_numpy()))
    keep = df[[key not in hit for key in map(tuple, df[["arm", "scenario", "rep"]].to_numpy())]]
    out = [f"=== without the {len(hit)} runs in which a JobManager was killed"]
    numbers.add(macro_name(tag, "nokill runs"), str(keep.groupby(["arm", "scenario", "rep"]).ngroups))
    for arm in [a for a in ARMS if (keep.arm == a).any()]:
        for a, b in PRIMARY_PAIRS:
            for col in [c for c in PRIMARY_COLS if c in keep]:
                r = pooled(keep, arm, a, b, col)
                if r:
                    base = (tag, "nokill pooled", arm, NAME[a], "vs", NAME[b], short[col])
                    for part in ("mean", "lo", "hi"):
                        numbers.add(macro_name(*base, part), f"{100 * r[part]:+.0f}".replace("+", "$+$").replace("-", "$-$"))
                    numbers.add(macro_name(*base, "n"), str(r["n"]))
                    numbers.add(macro_name(*base, "p raw"), f"{r['p']:.3f}")
                    out.append(f"  pooled {col:16s} {NAME[a]} - {NAME[b]}: {100 * r['mean']:+.1f}% [{100 * r['lo']:+.1f}, {100 * r['hi']:+.1f}] "
                               f"n={r['n']} p={r['p']:.3f}")
                for sc in present(keep, keep.arm == arm):
                    r = paired(keep, arm, sc, a, b, col)
                    if r:
                        base = (tag, "nokill diff", arm, sc, NAME[a], "vs", NAME[b], short[col])
                        for part in ("mean", "lo", "hi"):
                            numbers.add(macro_name(*base, part), num(r[part]))
                        numbers.add(macro_name(*base, "n"), str(r["n"]))
                        numbers.add(macro_name(*base, "wins"), str(r["wins"]))
                        out.append(f"  {sc:10s} {col:16s} {NAME[a]} - {NAME[b]}: {num(r['mean'])} [{num(r['lo'])}, {num(r['hi'])}] "
                                   f"lower in {r['wins']}/{r['n']}")
    return "\n".join(out)


def capacity_numbers(runs, numbers, tag):
    """What the rule measured as the capacity of one Aggregate subtask, by the parallelism it ran
    at: the mean over all decision cycles of the campaign. Capacity is close to linear, not exactly."""
    rows = []
    for (arm, scenario, rep), (meta, samples, latency, load, decisions) in runs.items():
        for d in decisions:
            if d.get("event") != "decision":
                continue
            for v in (d.get("vertices") or {}).values():
                if v.get("name") == "Aggregate" and v.get("capacity_per_subtask") and v.get("current"):
                    rows.append((int(v["current"]), float(v["capacity_per_subtask"])))
    if not rows:
        return ""
    c = pd.DataFrame(rows, columns=["p", "capacity"]).groupby("p").capacity.agg(["mean", "size"])
    for par in c.index:
        numbers.add(macro_name(tag, "capacity at", par), f"{c.loc[par, 'mean']:.0f}")
    return "=== capacity of one Aggregate subtask as the rule measured it, by parallelism\n" + c.round(1).to_string()


def decision_numbers(df, numbers, tag):
    """Macros for how often a controller chose what the policy in code chooses on the same inputs, per
    arm and over all arms, and for the built-in proposals the plugin dropped."""
    if "decisions_asked" not in df:
        return
    cols = ["decisions_asked", "decisions_agree", "decisions_applied_agree", "decisions_above_rule", "decisions_below_rule",
            "vetoed_scale_up_cycles", "vetoed_scale_down_cycles"]
    cols = [c for c in cols if c in df]
    scopes = [(arm, df[df.arm == arm]) for arm in ARMS if (df.arm == arm).any()] + [("all", df)]
    for scope, d in scopes:
        for job in [j for j in JOB_ORDER if ((d.job == j) & (d.decisions_asked > 0)).any()]:
            g = d[d.job == job][cols].sum()
            base = (tag, "decisions", scope, NAME[job])
            for c in cols:
                numbers.add(macro_name(*base, c.replace("decisions_", "").replace("_cycles", "").replace("_", " ")), f"{int(g[c]):,}")
            numbers.add(macro_name(*base, "pct"), f"{100 * g.decisions_agree / g.decisions_asked:.1f}")
            numbers.add(macro_name(*base, "applied pct"), f"{100 * g.decisions_applied_agree / g.decisions_asked:.1f}")


def decision_class_numbers(df, numbers, tag):
    """The comparison of decisions in the loop, per arm and controller: agreement with the policy by
    what the policy chose, with an interval over runs; deviations against the true requirement;
    cycles without a model decision; the latitude of the LLM's prompt; capped steps; and, for the
    built-in job, how often the built-in rule did what the policy would have done."""
    if "cycles" not in df:
        return ""
    out = ["=== decisions in the loop, by what the policy in code chose (percentages: all decisions of the arm; "
           "interval: over the runs that had such a decision)"]

    def total(d, col):
        return int(d[col].fillna(0).sum()) if col in d else 0

    for arm in [a for a in ARMS if (df.arm == a).any()]:
        for job in [j for j in JOB_ORDER if ((df.arm == arm) & (df.job == j)).any()]:
            d = df[(df.arm == arm) & (df.job == job)]
            key = (tag, "class", arm, NAME[job])
            if job.split("+")[0] == "builtin":
                if "obs_hold_n" not in d:
                    continue
                parts = []
                for cls in ("hold", "up", "down"):
                    n = total(d, f"obs_{cls}_n")
                    same, size = total(d, f"obs_{cls}_same_dir"), total(d, f"obs_{cls}_same_size")
                    less, more = total(d, f"obs_{cls}_smaller"), total(d, f"obs_{cls}_larger")
                    for name, v in (("n", n), ("same dir", same), ("same size", size), ("smaller", less), ("larger", more)):
                        numbers.add(macro_name(tag, "observe", arm, NAME[job], cls, name), f"{v:,}")
                    if n:
                        numbers.add(macro_name(tag, "observe", arm, NAME[job], cls, "dir pct"), f"{100 * same / n:.0f}")
                    parts.append(f"{cls} {same}/{n} same direction, {size}/{n} same size"
                                 + (f" ({less} smaller, {more} larger)" if cls == "up" else ""))
                out.append(f"  {arm:8s} {NAME[job]:8s} built-in rule against the policy on the same inputs (Aggregate): " + "; ".join(parts))
                continue
            if "dec_hold_n" not in d or not total(d, "cycles"):
                continue
            parts = []
            for cls in ("hold", "up", "down"):
                n, agree = total(d, f"dec_{cls}_n"), total(d, f"dec_{cls}_agree")
                numbers.add(macro_name(*key, cls, "n"), f"{n:,}")
                numbers.add(macro_name(*key, cls, "agree"), f"{agree:,}")
                if not n:
                    parts.append(f"{cls} 0")
                    continue
                per_run = (d[f"dec_{cls}_agree"] / d[f"dec_{cls}_n"])[d[f"dec_{cls}_n"] > 0].to_numpy(float)
                numbers.add(macro_name(*key, cls, "pct"), f"{100 * agree / n:.1f}")
                text = f"{cls} {agree}/{n} ({100 * agree / n:.1f}%"
                if len(per_run) > 1:
                    _, lo, hi = boot_ci(per_run)
                    numbers.add(macro_name(*key, cls, "lo"), f"{100 * max(lo, 0):.0f}")
                    numbers.add(macro_name(*key, cls, "hi"), f"{100 * min(hi, 1):.0f}")
                    text += f", by run [{100 * max(lo, 0):.0f}, {100 * min(hi, 1):.0f}]"
                parts.append(text + ")")
            out.append(f"  {arm:8s} {NAME[job]:8s} " + "; ".join(parts))
            names = [("dev_n", "deviations"), ("dev_above", "above"), ("dev_below", "below"), ("dev_towards", "towards"),
                     ("dev_away", "away"), ("dev_same", "same distance"), ("dev_unknown", "unknown")]
            for col, name in names:
                numbers.add(macro_name(tag, "deviations", arm, NAME[job], name), f"{total(d, col):,}")
            out.append(f"           Aggregate, deviations from the policy: {total(d, 'dev_n')} ({total(d, 'dev_above')} above, "
                       f"{total(d, 'dev_below')} below); against the true requirement {total(d, 'dev_towards')} towards, "
                       f"{total(d, 'dev_away')} away, {total(d, 'dev_same')} at the same distance, {total(d, 'dev_unknown')} unknown")
            names = [("cycles", "all"), ("cycles_fallback", "fallback"), ("cycles_failed_call", "failed call"),
                     ("cycles_circuit_open", "circuit open"), ("cycles_cooldown", "cooldown"),
                     ("vetoed_scale_up_cycles", "vetoed up"), ("vetoed_scale_down_cycles", "vetoed down")]
            for col, name in names:
                numbers.add(macro_name(tag, "cycles", arm, NAME[job], name), f"{total(d, col):,}")
            out.append(f"           cycles {total(d, 'cycles')}: fallbacks {total(d, 'cycles_fallback')} "
                       f"({total(d, 'cycles_failed_call')} failed calls, {total(d, 'cycles_circuit_open')} circuit open), "
                       f"in cooldown {total(d, 'cycles_cooldown')}; built-in proposals dropped: "
                       f"{total(d, 'vetoed_scale_up_cycles')} up, {total(d, 'vetoed_scale_down_cycles')} down")
            names = [("lat_cycles", "cycles"), ("lat_policy_up", "policy up"), ("lat_above", "above"),
                     ("lat_above_subtasks", "subtasks")]
            for col, name in names:
                numbers.add(macro_name(tag, "latitude", arm, NAME[job], name), f"{total(d, col):,}")
            numbers.add(macro_name(tag, "latitude", arm, NAME[job], "runs"), str(int((d.lat_cycles.fillna(0) > 0).sum())))
            out.append(f"           busy at least 95% and believed demand below capacity (Aggregate): {total(d, 'lat_cycles')} cycles in "
                       f"{int((d.lat_cycles.fillna(0) > 0).sum())} of {len(d)} runs; the policy scaled up in {total(d, 'lat_policy_up')}; "
                       f"chose above the policy in {total(d, 'lat_above')}, by {total(d, 'lat_above_subtasks')} subtasks in all")
            if "lat_at_max" in d:
                numbers.add(macro_name(tag, "latitude", arm, NAME[job], "at max"), f"{total(d, 'lat_at_max'):,}")
                out.append(f"           the same while Aggregate ran at the maximum, where no model is asked: {total(d, 'lat_at_max')} cycles")
            if "olat_cycles" in d:
                for col, name in [("olat_cycles", "other cycles"), ("olat_policy_up", "other policy up"),
                                  ("olat_above", "other above"), ("olat_above_subtasks", "other subtasks")]:
                    numbers.add(macro_name(tag, "latitude", arm, NAME[job], name), f"{total(d, col):,}")
                out.append(f"           the same on the other vertex (not in the plan's count): {total(d, 'olat_cycles')} cycles in "
                           f"{int((d.olat_cycles.fillna(0) > 0).sum())} of {len(d)} runs; the policy scaled up in "
                           f"{total(d, 'olat_policy_up')}; chose above the policy in {total(d, 'olat_above')}, by "
                           f"{total(d, 'olat_above_subtasks')} subtasks in all")
            names = [("policy_ups", "policy ups"), ("policy_ups_capped", "policy ups capped"),
                     ("policy_ups_doubled", "policy ups doubled"), ("top_chosen", "top chosen"),
                     ("capped_total", "total limit")]
            for col, name in names:
                numbers.add(macro_name(tag, "capped", arm, NAME[job], name), f"{total(d, col):,}")
            out.append(f"           scale-ups of Aggregate by the policy: {total(d, 'policy_ups')}, of which the limit of four times "
                       f"the parallelism capped {total(d, 'policy_ups_capped')} and the projection had doubled the demand in "
                       f"{total(d, 'policy_ups_doubled')}; chose the largest option under that limit "
                       f"{total(d, 'top_chosen')} times; trimmed by the total limit {total(d, 'capped_total')}")
    return "\n".join(out)


MODELS_BOUND_S = 70.0  # half of the smaller effect of the signal in this scenario (E8b: -141 s with the stock band)


def overload_numbers(df, numbers, tag):
    """E10 (plan amendment of 2026-11-18 10:00 PST): the models' contribution in seconds, paired
    within runs, against half of the smaller effect of the signal in the same scenario (E8b), and
    the effect of the signal inside the campaign per controller (separate runs, Welch)."""
    if tag != "e10":
        return ""
    bound = MODELS_BOUND_S
    rule = next((j for j in JOB_ORDER if j.split("+")[0] == "rule"), None)
    out = [f"=== the models' contribution in seconds over the limit, against plus or minus {bound:.0f} s"]
    for arm in [a for a in ARMS if (df.arm == a).any()]:
        worst, verdict = 0.0, True
        for job in [j for j in JOB_ORDER if analyze.has_model(j)]:
            for sc in present(df, df.arm == arm):
                r = paired(df, arm, sc, job, rule, "slo_violation_s")
                if not r:
                    verdict = False
                    continue
                inside = -bound <= r["lo"] and r["hi"] <= bound
                verdict = verdict and inside
                worst = max(worst, abs(r["lo"]), abs(r["hi"]))
                numbers.add(macro_name(tag, "small", arm, NAME[job]), "yes" if inside else "no")
                out.append(f"  {arm:8s} {NAME[job]} - {NAME[rule]}: {num(r['mean'])} [{num(r['lo'])}, {num(r['hi'])}] s, "
                           f"n={r['n']}, lower in {r['wins']}: {'within' if inside else 'NOT within'}")
        numbers.add(macro_name(tag, "small", arm, "both"), "yes" if verdict else "no")
        numbers.add(macro_name(tag, "small", arm, "allows"), f"{worst:.0f}")
    numbers.add(macro_name(tag, "small", "bound"), f"{bound:.0f}")
    if len(ARMS) == 2 and set(ARMS) <= set(df.arm):
        short = {"slo_violation_s": "slo", "slots_mean": "slots", "scale_ups": "ups", "time_to_full_s": "full"}
        for sc in present(df):
            for job in [j for j in JOB_ORDER if (df.job == j).any()]:
                for col, key in short.items():
                    r = unpaired(df, sc, job, col) if col in df else None
                    if r:
                        for part in ("mean", "lo", "hi"):
                            numbers.add(macro_name(tag, "signal effect", sc, NAME[job], key, part),
                                        whole(r[part]) if key in ("slo", "full") else num(r[part]))
                        numbers.add(macro_name(tag, "signal effect", sc, NAME[job], key, "n"), f"{r['n'][1]} and {r['n'][0]}")
    return "\n".join(out)


OVERLOAD_COLS = [("slo_violation_s", "Over limit (s)"), ("slots_mean", "Slots"), ("scale_ups", "Steps"),
                 ("time_to_full_s", "Full (s)"), ("scaling_actions", "Resc.")]


def table_overload(df, path, numbers, tag):
    """E10: one row per controller and one block per arm. Steps are the scale-ups of Aggregate, Full
    is the time from the start of the overload until Aggregate runs at the maximum parallelism, and
    Resc. counts every rescaling of the job in the measured phase. Also the numbers on steps that
    the text quotes, and the agreement of the models with the policy by vertex."""
    arms = [a for a in ARMS if (df.arm == a).any()]
    jobs = [j for j in JOB_ORDER if (df.job == j).any()]
    width = len(OVERLOAD_COLS)
    lines = ["\\begin{tabular}{@{}l" + ("r" * width * len(arms)) + "@{}}", "\\toprule"]
    lines.append("".join(f" & \\multicolumn{{{width}}}{{c}}{{{ARM_NAME[a].capitalize()} ($n$={df[df.arm == a].rep.nunique()})}}"
                         for a in arms) + " \\\\")
    lines.append("".join(f"\\cmidrule(lr){{{2 + width * i}-{1 + width * (i + 1)}}}" for i in range(len(arms))))
    lines.append("Controller" + "".join(f" & {t}" for _ in arms for _, t in OVERLOAD_COLS) + " \\\\")
    lines.append("\\midrule")
    out = ["=== the overload campaign: steps of Aggregate and time until it runs at the maximum parallelism"]
    for job in jobs:
        cells = []
        for arm in arms:
            d = df[(df.arm == arm) & (df.job == job)]
            for col, _ in OVERLOAD_COLS:
                x = d[col].dropna()
                mean, lo, hi = boot_ci(x)
                txt = half_up(mean, 1) if col in ("scale_ups", "scaling_actions", "slots_mean") else num(mean, 0)
                if col == "slo_violation_s" and len(x) > 1:
                    txt += f" {{\\tiny [{num(lo, 0)}, {num(hi, 0)}]}}"
                cells.append(txt)
            ups = d.scale_ups.dropna()
            numbers.add(macro_name(tag, "steps", arm, NAME[job], "ups"), half_up(ups.mean(), 1))
            numbers.add(macro_name(tag, "steps", arm, NAME[job], "full"), num(d.time_to_full_s.mean(), 0))
            numbers.add(macro_name(tag, "steps", arm, NAME[job], "three runs"), str(int((ups >= 3).sum())))
            numbers.add(macro_name(tag, "steps", arm, NAME[job], "two runs"), str(int((ups == 2).sum())))
            numbers.add(macro_name(tag, "steps", arm, NAME[job], "actions"), half_up(d.scaling_actions.mean(), 1))
            out.append(f"  {arm:8s} {NAME[job]:8s} scale-ups {ups.mean():.2f} (three or more in {int((ups >= 3).sum())} of "
                       f"{len(ups)} runs), full after {d.time_to_full_s.mean():.0f} s, rescalings {d.scaling_actions.mean():.2f}")
        lines.append(f"{NAME[job]} & " + " & ".join(cells) + " \\\\")
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")
    rule = next((j for j in JOB_ORDER if j.split("+")[0] == "rule"), None)
    for arm in arms:
        for sc in present(df, df.arm == arm):
            for job in [j for j in jobs if analyze.has_model(j)]:
                r = paired(df, arm, sc, job, rule, "time_to_full_s")
                if r:
                    for part in ("mean", "lo", "hi"):
                        numbers.add(macro_name(tag, "late", arm, NAME[job], part), num(r[part], 0))
                    out.append(f"  {arm:8s} {NAME[job]} - {NAME[rule]}, time until full parallelism: {num(r['mean'])} "
                               f"[{num(r['lo'])}, {num(r['hi'])}] s, n={r['n']}")
    numbers.add(macro_name(tag, "runs all"), str(df.groupby(["arm", "scenario", "rep"]).ngroups))
    if "agg_n" in df:
        for job in [j for j in jobs if analyze.has_model(j)]:
            d = df[df.job == job]
            for part, name in (("agg", "aggregate"), ("oth", "other")):
                n, agree = int(d[f"{part}_n"].sum()), int(d[f"{part}_agree"].sum())
                applied = int(d[f"{part}_applied_agree"].sum())
                numbers.add(macro_name(tag, name, NAME[job], "n"), f"{n:,}")
                numbers.add(macro_name(tag, name, NAME[job], "agree"), f"{agree:,}")
                numbers.add(macro_name(tag, name, NAME[job], "differ"), f"{n - agree:,}")
                numbers.add(macro_name(tag, name, NAME[job], "applied differ"), f"{n - applied:,}")
                out.append(f"  {NAME[job]:4s} {name:9s} (both arms): the policy's option in {agree} of {n} decisions; "
                           f"applied decision differs from the policy's in {n - applied}")
    return "\n".join(out)


def digest(df, path):
    out = []
    for arm in [a for a in ARMS if (df.arm == a).any()]:
        out.append(f"=== arm: {arm}")
        for sc in present(df, df.arm == arm):
            d = df[(df.arm == arm) & (df.scenario == sc)]
            out.append(f"--- {sc} (n={d.rep.nunique()}, drained at start in {int(d.groupby('rep').drained_at_start.all().sum())})")
            for col in ("slo_violation_s", "slots_mean", "latency_p95_s", "lag_max_records", "lag_mean_records",
                        "parallelism_total_mean", "tm_pods_mean", "scaling_actions", "direction_reversals",
                        "first_action_s", "busy_before_event", "under_timeshare_pct", "over_accuracy_pct"):
                cells = []
                for job in [j for j in JOB_ORDER if (d.job == j).any()]:
                    mean, lo, hi = boot_ci(d[d.job == job][col])
                    cells.append(f"{NAME[job]} {num(mean)} [{num(lo)}, {num(hi)}]")
                out.append(f"  {col:26s} " + " | ".join(cells))
            for col in ("slo_violation_s", "slots_mean", "latency_p95_s"):
                for a, b in contrasts():
                    r = paired(df, arm, sc, a, b, col)
                    if r:
                        out.append(f"  paired {col:18s} {NAME[a]:18s} - {NAME[b]:14s}: {num(r['mean'])} [{num(r['lo'])}, {num(r['hi'])}] "
                                   f"p={r['p']:.3f} lower in {r['wins']}/{r['n']}")
        tests = pooled_tests(df, arm)
        if tests:
            family = sum(1 for t in tests if t[4] is not None)
            out.append(f"--- pooled over scenarios, {arm}: relative difference (fraction of the scenario mean); "
                       f"Holm-adjusted p for the {family} primary tests, the others are descriptive")
            for a, b, col, r, adj in tests:
                test = f"p={r['p']:.4f} adjusted {adj:.4f}" if adj is not None else "(not a primary contrast)"
                out.append(f"  {col:18s} {NAME[a]:18s} - {NAME[b]:14s}: {100 * r['mean']:+.0f}% [{100 * r['lo']:+.0f}, {100 * r['hi']:+.0f}] "
                           f"n={r['n']} {test}")
    if "decisions_asked" in df:
        out.append("=== decisions: how often a controller chose what the policy in code chooses on the same inputs")
        g = df[df.decisions_asked > 0].groupby(["arm", "job"])[["decisions_asked", "decisions_agree", "decisions_applied_agree",
                                                                "decisions_above_rule", "decisions_below_rule"]].sum()
        g["agree_pct"] = (100 * g.decisions_agree / g.decisions_asked).round(1)
        out.append(g.to_string())
    if len(ARMS) == 2 and set(ARMS) <= set(df.arm):
        out.append(f"=== effect of the second arm ({ARMS[1]} minus {ARMS[0]}, runs not paired, Welch's t interval)")
        for sc in present(df):
            for job in [j for j in JOB_ORDER if (df.job == j).any()]:
                for col in [c for c in ("slo_violation_s", "slots_mean", "latency_p95_s", "scale_ups", "time_to_full_s") if c in df]:
                    r = unpaired(df, sc, job, col)
                    if r:
                        out.append(f"  {sc:11s} {NAME[job]:7s} {col:24s} {num(r['mean'])} [{num(r['lo'])}, {num(r['hi'])}]")
    if "backlog_seen_ratio" in df:
        out.append("=== what the autoscaler saw while a real backlog existed (median ratio believed / true)")
        g = df[df.overload_cycles > 0].groupby(["arm", "job"])[["backlog_seen_ratio", "demand_seen_ratio"]].mean()
        out.append(g.round(3).to_string())
    for job in [j for j in JOB_ORDER if analyze.has_model(j)]:
        d = df[df.job == job]
        if len(d):
            out.append(f"=== model use, {NAME[job]}: {int(d.model_calls.sum())} calls, mean latency "
                       f"{d.model_latency_ms_mean.mean():.0f} ms, slowest {d.model_latency_ms_max.max():.0f} ms, "
                       f"${d.model_cost_usd_per_hour.mean():.4f} per job-hour, {int(d.model_fallbacks.sum())} fallbacks")
    open(path, "w").write("\n".join(out) + "\n")
    return "\n".join(out)


def configure(experiment):
    """Sets the module's view of one campaign: its arms, its jobs with their names, colours and
    markers, the reference job and the primary family of tests."""
    global ARMS, ARM_NAME, JOB_ORDER, NAME, MARK, COLOR, REFERENCE, PAIRS, PRIMARY_PAIRS, PRIMARY_COLS, ALIAS, POOL
    global POOL_SCENARIOS
    exp = EXPERIMENTS[experiment]
    ALIAS = dict(exp.get("alias", {}))
    POOL = bool(exp.get("pool", True))
    POOL_SCENARIOS = exp.get("pool_scenarios")
    ARMS, ARM_NAME = tuple(exp["arms"]), dict(exp["arms"])
    JOB_ORDER = [j for j, *_ in exp["jobs"]]
    NAME = {j: n for j, n, _, _ in exp["jobs"]}
    COLOR = {j: c for j, _, c, _ in exp["jobs"]}
    MARK = {j: m for j, _, _, m in exp["jobs"]}
    REFERENCE = exp.get("reference")
    key = {n: j for j, n in NAME.items()}
    PAIRS = [(key[a], key[b]) for a, b in exp.get("pairs", [])]
    PRIMARY_PAIRS = [(key[a], key[b]) for a, b in exp.get("primary", [])]
    PRIMARY_COLS = exp.get("primary_cols", [c for c, _ in PRIMARY])
    POOLED.clear()
    return exp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("experiment", choices=sorted(EXPERIMENTS) + ["offline", "signal"])
    ap.add_argument("--root", default=None)
    ap.add_argument("--out", default=os.path.join(ROOT, "paper"))
    args = ap.parse_args()
    if args.experiment == "offline":
        # The offline study has no runs on the cluster: only its table and numbers.
        os.makedirs(os.path.join(args.out, "gen"), exist_ok=True)
        numbers = Numbers()
        table_offline(os.path.join(args.out, "gen", "tab_offline.tex"), numbers)
        numbers.write(os.path.join(args.out, "gen", "numbers_offline.tex"))
        return
    if args.experiment == "signal":
        # The table over the closed-loop campaigns on the backlog signal. Run the analysis of each
        # campaign first: it writes the runs.csv this reads.
        os.makedirs(os.path.join(args.out, "gen"), exist_ok=True)
        numbers = Numbers()
        print(table_closed(os.path.join(args.out, "gen", "tab_signal_closed.tex"), numbers))
        numbers.write(os.path.join(args.out, "gen", "numbers_signal.tex"))
        return
    exp = configure(args.experiment)
    root = args.root or os.path.join(ROOT, exp["root"])
    tag = args.experiment
    gen, figs = os.path.join(args.out, "gen"), os.path.join(args.out, "figs")
    os.makedirs(gen, exist_ok=True)
    os.makedirs(figs, exist_ok=True)
    df, runs = collect(root)
    if not len(df):
        raise SystemExit(f"no finished runs under {root}")
    df = df[df.job.isin(JOB_ORDER)]
    numbers = Numbers()
    for arm in ARMS:
        numbers.add(macro_name(tag, "runs", arm), str(df[df.arm == arm].groupby(["scenario", "rep"]).ngroups))
    numbers.add(macro_name(tag, "repsMin"), str(int(df.groupby(["arm", "scenario"]).rep.nunique().min())))
    numbers.add(macro_name(tag, "repsMax"), str(int(df.groupby(["arm", "scenario"]).rep.nunique().max())))
    table_main(df, os.path.join(gen, f"tab_{tag}.tex"), numbers, tag)
    table_signal(df, os.path.join(gen, f"tab_{tag}_signal.tex"), numbers, tag)
    table_models(df, os.path.join(gen, f"tab_{tag}_models.tex"), numbers, tag)
    contrast_numbers(df, numbers, tag)
    decision_numbers(df, numbers, tag)
    extra = noise_numbers(df, root, numbers, tag) if tag in ("e2", "e2b", "e2c") else equivalence_numbers(df, numbers, tag)
    extra = "\n".join(x for x in (extra, fault_numbers(df, numbers, tag), kill_sensitivity(df, numbers, tag),
                                  decision_class_numbers(df, numbers, tag), overload_numbers(df, numbers, tag),
                                  table_overload(df, os.path.join(gen, f"tab_{tag}_overload.tex"), numbers, tag)
                                  if tag == "e10" else "",
                                  capacity_numbers(runs, numbers, tag)) if x)
    numbers.write(os.path.join(gen, f"numbers_{tag}.tex"))
    scenario = "spike" if (df.scenario == "spike").any() else df.scenario.iloc[0]
    if tag in ("e8", "e8b", "e8c", "e8d"):
        table_sizing(df, os.path.join(gen, f"tab_{tag}_sizing.tex"), numbers, tag)
        if tag == "e8":
            first_step_numbers(df, numbers, tag)
        if tag in ("e8b", "e8c", "e8d"):
            ups_path = os.path.join(root, "scale_ups.csv")
            ups = pd.read_csv(ups_path) if os.path.exists(ups_path) else pd.DataFrame()
            # One set of numbers per scenario; a campaign with one scenario keeps the plain tag.
            for sc in present(df):
                part = tag if df.scenario.nunique() == 1 else f"{tag} {sc.split('_')[-1]}"
                table_surge(df[df.scenario == sc], ups[ups.scenario == sc] if len(ups) else ups,
                            os.path.join(gen, f"tab_{part.replace(' ', '_')}.tex"), numbers, part)
            most = df.groupby("scenario").rep.nunique().idxmax()
            shown = fig_surge(runs, df, os.path.join(figs, f"{tag}_surge.pdf"), ("builtin", "builtin+lagfix"),
                              scenario=most)
            fig_surge(runs, df, os.path.join(figs, f"{tag}_surge_target.pdf"), ("builtin+max70", "builtin+max70+lagfix"),
                      scenario=most)
            numbers.add(macro_name(tag, "surge", "figure rep"), str(shown))
        numbers.write(os.path.join(gen, f"numbers_{tag}.tex"))
        shown = [j for j in JOB_ORDER if (df.job == j).any() and (tag != "e8" or j != "builtin")]
        fig_signal(runs, os.path.join(figs, f"{tag}_signal.pdf"), scenario, jobs=shown if tag == "e8" else shown[:2])
    else:
        fig_signal(runs, os.path.join(figs, f"{tag}_signal.pdf"), scenario)
    fig_timeline(runs, os.path.join(figs, f"{tag}_timeline.pdf"), scenario)
    fig_summary(df, os.path.join(figs, f"{tag}_summary.pdf"))
    fig_pareto(df, os.path.join(figs, f"{tag}_pareto.pdf"))
    text = digest(df, os.path.join(gen, f"summary_{tag}.txt"))
    # Disturbances of the testbed that no controller asked for (Issue #61).
    hit = df[df.jm_restarts > 0]
    numbers.add(macro_name(tag, "jm restarts"), str(int(hit.jm_restarts.sum())))
    numbers.add(macro_name(tag, "jm restart runs"), str(hit.groupby(["arm", "scenario", "rep"]).ngroups))
    numbers.write(os.path.join(gen, f"numbers_{tag}.tex"))
    lines = [f"=== JobManager restarts in the measured phase: {int(hit.jm_restarts.sum())} in "
             f"{hit.groupby(['arm', 'scenario', 'rep']).ngroups} of {df.groupby(['arm', 'scenario', 'rep']).ngroups} runs"]
    lines += [f"  {r.arm} {r.scenario} rep {r.rep}: {NAME.get(r.job, r.job)}" for r in hit.itertuples()]
    extra = "\n".join(x for x in (extra, "\n".join(lines)) if x)
    if extra:
        text += "\n" + extra
        open(os.path.join(gen, f"summary_{tag}.txt"), "a").write(extra + "\n")
    print(text)


if __name__ == "__main__":
    main()
