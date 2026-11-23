#!/usr/bin/env python3
"""Turns the raw benchmark output into metrics, tables and charts.

Usage:
  python3 bench/analyze.py [--results results]

Writes:
  <results>/summary.csv           one row per scenario, repetition and autoscaler
  <results>/summary.md            mean (min-max) over repetitions, per scenario
  <results>/charts/<scenario>.png timeline of every repetition
  <results>/charts/summary.png    headline metrics side by side
"""

import argparse
import glob
import json
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

JOBS = ("builtin", "jev")  # replaced in main() by the jobs the runs actually contain


class _PerJob(dict):
    """Names and colours for the fixed jobs, with rules for the variants (builtinNN, llm-<tier>)."""

    def __init__(self, fixed, variant):
        super().__init__(fixed)
        self.variant = variant

    def __missing__(self, job):
        return self.variant(job)


def options(job):
    """The options of a job spec as a dict, e.g. 'builtin+max70+sp2' -> {'max': 70, 'sp': 2}."""
    out = {}
    for o in job.split("+")[1:]:
        m = re.fullmatch(r"([a-z]+?)(\d+)", o)
        out[m.group(1) if m else o] = int(m.group(2)) if m else True
    return out


def safe(job):
    """The job spec as it appears in Kubernetes names and therefore in the operator log."""
    return job.replace("+", "-")


def _describe(job, short):
    base, o = job.split("+")[0], options(job)
    name = {"builtin": "Built-in", "rule": "Rule", "jev": "Jev", "llm": "Claude"}[base] if short else {
        "builtin": "Built-in autoscaler", "rule": "Plugin policy in code (no model)", "jev": "Jev decision model",
        "llm": "Claude Haiku"}[base]
    parts = []
    for key, text in (("max", "max {}%"), ("min", "min {}%"), ("t", "target {}%"), ("f", "fetch {} B"),
                      ("rd", "restore +{} s"), ("sp", "source x{}")):
        if key in o:
            parts.append(text.format(o[key]))
    parts += [k for k in ("lagfix", "noplugin", "off", "sonnet", "opus", "fable") if k in o]
    parts += [k for k, v in o.items() if len(k) == 1 and v is True]
    return name + (" (" + ", ".join(parts) + ")" if parts else "")


_PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
_BASE_COLOR = {"builtin": "#2a78d6", "jev": "#eb6834", "llm": "#1baf7a", "rule": "#eda100"}
LABEL = _PerJob({}, lambda j: _describe(j, False))
SHORT = _PerJob({}, lambda j: _describe(j, True))
# A plain controller keeps its colour in every chart; variants of one take the next free slots.
COLOR = _PerJob({}, lambda j: _BASE_COLOR[j] if j in _BASE_COLOR else _PALETTE[(sum(map(ord, j)) % 4) + 4])


def has_model(job):
    return job.split("+")[0] in ("jev", "llm")


INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
VERTICES = ("source", "aggregate", "measure")

SLO_MS = 2000  # a second counts as "bad" when its p95 latency is above this
RECOVERED_MS = 1000  # latency level that counts as recovered ...
RECOVERED_HOLD_S = 30  # ... once it has held for this long
PRICE_PER_MTOK = 0.042  # Jev: USD per million input tokens; output tokens are free
CLAUDE_PRICE = {  # USD per million input, output tokens
    "claude-haiku-5-5": (0.10, 0.50), "claude-sonnet-5-5": (2.00, 10.00),
    "claude-opus-5-5": (4.00, 20.00), "claude-fable-5-2": (8.00, 40.00),
}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 9,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "lines.linewidth": 1.6, "legend.frameon": False,
})


def weighted_quantile(values, weights, q):
    values, weights = np.asarray(values, float), np.asarray(weights, float)
    if len(values) == 0 or weights.sum() <= 0:
        return float("nan")
    order = np.argsort(values)
    cum = np.cumsum(weights[order])
    return float(values[order][np.searchsorted(cum, q * cum[-1])])


def load_run(path):
    meta = json.load(open(os.path.join(path, "meta.json")))
    samples = pd.read_csv(os.path.join(path, "samples.csv"))
    latency = pd.read_csv(os.path.join(path, "latency.csv"))
    load = pd.read_csv(os.path.join(path, "load.csv"))
    decisions = []
    dpath = os.path.join(path, "decisions.jsonl")
    if os.path.exists(dpath):
        for line in open(dpath):
            try:
                decisions.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return meta, samples, latency, load, decisions


def per_second(latency, job, duration):
    """Per-second latency and throughput for one job, indexed by whole seconds of the run."""
    d = latency[latency.job == job].copy()
    d["sec"] = np.floor(d.t).astype(int)
    g = d.groupby("sec").agg(count=("count", "sum"), p50=("p50_ms", "median"),
                             p95=("p95_ms", "max"), p99=("p99_ms", "max"), mx=("max_ms", "max"))
    return g.reindex(range(int(d.sec.min()) if len(d) else 0, duration + 1))


def healthy_series(p95):
    """p95 per second with short gaps (up to 3 s without output) filled from the previous second.
    Longer gaps stay empty and count as a stall."""
    return p95.ffill(limit=3)


def recovery_seconds(sec, events, duration):
    """Seconds from each workload change until the last second with p95 latency above the
    recovered level (or with no output), provided latency then stays healthy for the hold period.
    If it never does before the next change, the whole interval counts. Summed over the events."""
    total = 0.0
    bounds = list(events) + [duration]
    for i, e in enumerate(events):
        window = healthy_series(sec.p95).loc[e:bounds[i + 1] - 1]
        bad = (window.isna() | (window > RECOVERED_MS)).to_numpy()
        if not bad.any():
            continue
        last_bad = int(np.flatnonzero(bad)[-1])
        healthy_tail = len(bad) - 1 - last_bad
        total += last_bad + 1 if healthy_tail >= min(RECOVERED_HOLD_S, len(bad) // 2) else len(bad)
    return total


def seconds(text):
    """A Flink duration such as 30s, 1m, 2 min or 1h, in seconds."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-z]*)\s*", str(text).lower())
    if not m:
        raise ValueError(f"not a duration: {text}")
    unit = m.group(2)
    scale = 0.001 if unit == "ms" else 60 if unit.startswith("m") else 3600 if unit.startswith("h") else 1
    return float(m.group(1)) * scale


STALE_FALLBACK_S = 4.9  # median over the 60 job-runs of results-e1, sampled once every 5 s


def reader_staleness(s, meta):
    """Seconds by which Flink's source metrics in a sample are older than the broker end offsets of
    the same sample. Flink's REST API answers a metric query from the snapshot the previous query
    asked for (MetricFetcher.update() is asynchronous), so a sampler that asks once per interval
    gets values one interval old. Estimated from the samples before the first workload change,
    where the job is caught up: position(t) = end offsets(t - staleness)."""
    fallback = 0.0 if meta.get("metric_refresh") else STALE_FALLBACK_S
    if "position_offset" not in s or not s.position_offset.notna().any():
        return fallback
    events = meta.get("events") or []
    upto = events[0] - 3 if events else 0  # the recorded event times include the run's shift
    c = s.assign(position_offset=valid_position(s))
    c = c[(c.t >= -25) & (c.t < upto)].dropna(subset=["position_offset", "end_offset"])
    if len(c) < 4 or c.t.iloc[-1] - c.t.iloc[0] < 10:
        return fallback
    rate = (c.end_offset.iloc[-1] - c.end_offset.iloc[0]) / (c.t.iloc[-1] - c.t.iloc[0])
    if rate <= 0:
        return fallback
    delta = float(((c.end_offset - c.position_offset) / rate).median())
    # More than a few seconds means the job was not caught up, and this is not a staleness.
    return delta if -1.0 <= delta <= 7.0 else fallback


def true_backlog(s, meta):
    """(t, records): the broker end offsets minus the reader's position, for all samples of one job.
    Each value is the backlog at the time its position was taken, which is `t`."""
    delta = reader_staleness(s, meta)
    known = s.dropna(subset=["end_offset"])
    end_then = np.interp((s.t - delta).to_numpy(float), known.t.to_numpy(float), known.end_offset.to_numpy(float))
    lag = np.clip(end_then - position_estimate(s), 0, None)
    return (s.t - delta).to_numpy(float), lag


def valid_position(s):
    """The reader position without the samples taken while a job (re)starts. A reader never is behind
    the offsets it committed at its last checkpoint; a smaller sum means that some partitions had not
    reported a position yet."""
    pos = s.position_offset.copy()
    if "committed_offset" in s:
        pos[pos < s.committed_offset.ffill()] = np.nan
    return pos


def position_estimate(s):
    """The reader position for every sample. After a restart a partition reports a position only
    once it has emitted a record, which for a slow job with a large backlog takes a minute or more:
    the consumer hands out one partition's fetch before the next. For those samples the position is
    interpolated between the valid ones before and after, and is never taken to be behind the
    offsets committed at the last checkpoint. Holding the last valid position instead would let the
    backlog grow with the arrivals while the job is in fact reading."""
    pos = valid_position(s).to_numpy(float)
    t = s.t.to_numpy(float)
    ok = ~np.isnan(pos)
    if ok.sum() >= 2:
        est = np.interp(t, t[ok], pos[ok])
    else:
        est = pd.Series(pos).ffill().bfill().to_numpy(float)
    if "committed_offset" in s:
        est = np.fmax(est, s.committed_offset.ffill().to_numpy(float))
    return est


def metrics(meta, samples, latency, load, decisions, job):
    dur = meta["duration"]
    sec = per_second(latency, job, dur)
    phase = sec.loc[0:dur]
    lat = latency[(latency.job == job) & (latency.t >= 0) & (latency.t <= dur)]
    s = samples[(samples.job == job) & (samples.t >= 0) & (samples.t <= dur)].copy()
    sent = load[load.t >= 0]
    sent_n = (sent.sent_total.max() - sent.sent_total.min()) if len(sent) else float("nan")

    par = s[[f"p_{v}" for v in VERTICES]].ffill().bfill()
    total_par = par.sum(axis=1)
    # Scaling actions are counted from the parallelism the autoscaler asked for, not from the
    # observed one, which passes through intermediate values while TaskManagers start.
    asked = samples[samples.job == job][[f"target_p_{v}" for v in VERTICES]].apply(pd.to_numeric, errors="coerce").ffill()
    asked = asked.loc[s.index]
    asked.columns = par.columns
    asked = asked.fillna(par.iloc[0] if len(par) else 0)
    changes = asked.diff().fillna(0)
    actions = int((changes.abs().sum(axis=1) > 0).sum())
    agg_steps = changes["p_aggregate"][changes["p_aggregate"] != 0].to_numpy()
    reversals = int(np.sum(np.sign(agg_steps[1:]) != np.sign(agg_steps[:-1]))) if len(agg_steps) > 1 else 0
    # True backlog: broker end offsets minus the reader's position; older runs only have the
    # committed offsets, which are up to one checkpoint interval behind.
    if "position_offset" in s and s.position_offset.notna().any():
        t_all, lag_all = true_backlog(samples[samples.job == job], meta)
        lag = pd.Series(np.interp(s.t.to_numpy(float), t_all, lag_all), index=s.index)
    else:
        lag = (s.end_offset - s.committed_offset).clip(lower=0)
    # Reaction time: from the first workload change to the first scaling action at or after it.
    first_event = meta["events"][0] if meta["events"] else 0
    acted = s.t[(changes.abs().sum(axis=1) > 0) & (s.t >= first_event)]

    healthy = healthy_series(sec.p95).loc[0:dur]
    over = healthy.isna() | (healthy > SLO_MS)
    out = {
        "latency_p50_ms": weighted_quantile(lat.p50_ms, lat["count"], 0.5),
        "latency_p95_ms": weighted_quantile(lat.p95_ms, lat["count"], 0.95),
        "latency_p99_ms": weighted_quantile(lat.p99_ms, lat["count"], 0.99),
        "latency_max_ms": float(lat.max_ms.max()) if len(lat) else float("nan"),
        "slo_violation_s": int(over.sum()),
        # Still over the limit in the last ten seconds of the measured phase: the count is then a
        # lower bound. And the last second over the limit, to tell a job that caught up from one
        # that recovered only because the load fell.
        "slo_censored": bool(len(over) >= 10 and over.iloc[-10:].all()),
        "slo_last_s": float(over.index[over.to_numpy()][-1]) if over.any() else float("nan"),
        "recovery_s": recovery_seconds(sec, meta["events"], dur),
        "throughput_rps": float(phase["count"].sum() / dur),
        "processed_over_sent": float(phase["count"].sum() / sent_n) if sent_n else float("nan"),
        "lag_max_records": float(lag.max()),
        "lag_mean_records": float(lag.mean()),
        "parallelism_total_mean": float(total_par.mean()),
        # With slot sharing a job occupies as many slots as its largest vertex has subtasks.
        "slots_mean": float(par.max(axis=1).mean()),
        "busy_before_event": float(s.busy_aggregate[(s.t >= first_event - 30) & (s.t < first_event)].mean()),
        "parallelism_aggregate_mean": float(par.p_aggregate.mean()),
        "parallelism_aggregate_max": float(par.p_aggregate.max()),
        "tm_pods_mean": float(s.tm_pods.mean()),
        "scaling_actions": actions,
        "direction_reversals": reversals,
        "first_action_s": float(acted.iloc[0] - first_event) if len(acted) else float("nan"),
        "not_running_samples": int((s.state != "RUNNING").sum()),
        "drained_at_start": bool(meta.get("drained", True)),
    }
    mine = [d for d in decisions if f"name='bench-{safe(job)}'" in d.get("job", "")]
    # Decision-level comparison with the policy applied in code, on the inputs of each decision.
    asked = [v for d in mine if d.get("event") == "decision" for v in (d.get("vertices") or {}).values()
             if v.get("choice") and v.get("rule_choice")]
    if asked and job.split("+")[0] != "builtin":
        out.update({
            "decisions_asked": len(asked),
            "decisions_agree": sum(1 for v in asked if v["choice"] == f"p{v['rule_choice']}"),
            "decisions_applied_agree": sum(1 for v in asked if v["target"] == v["rule_choice"]),
            "decisions_above_rule": sum(1 for v in asked if int(v["choice"][1:]) > v["rule_choice"]),
            "decisions_below_rule": sum(1 for v in asked if int(v["choice"][1:]) < v["rule_choice"]),
        })
    # Cycles in which the plugin dropped a proposal of the built-in rule (the plan was to hold, or a
    # cooldown was active), by the direction of the proposal for the Aggregate vertex or any vertex.
    vetoes = [d for d in mine if d.get("event") == "apply" and d.get("mode") == "veto" and d.get("built_in")]
    if job.split("+")[0] != "builtin":
        ups = sum(1 for d in vetoes if any(int(x.split("->")[1]) > int(x.split("->")[0]) for x in d["built_in"].values()))
        out.update({"vetoed_scale_up_cycles": ups, "vetoed_scale_down_cycles": len(vetoes) - ups})
    if has_model(job):
        calls = [d for d in mine if d.get("event") == "decision" and d.get("input_tokens")]
        tokens = sum(d["input_tokens"] for d in calls)
        out_tokens = sum(d.get("output_tokens") or 0 for d in calls)
        if job == "jev":
            cost = tokens * PRICE_PER_MTOK / 1e6
        else:
            conf = (meta.get("jobs_conf") or {}).get(job) or meta.get("llm") or {}
            pin, pout = CLAUDE_PRICE[conf.get("job.autoscaler.jev.llm.model", "claude-haiku-5-5")]
            cost = (tokens * pin + out_tokens * pout) / 1e6
        run_hours = (meta["duration"] + meta["warmup"]) / 3600
        out.update({
            "model_calls": len(calls),
            "model_input_tokens": tokens,
            "model_output_tokens": out_tokens,
            "model_cost_usd": cost,
            "model_cost_usd_per_hour": cost / run_hours,
            "model_latency_ms_mean": float(np.mean([d["latency_ms"] for d in calls])) if calls else float("nan"),
            "model_latency_ms_max": float(np.max([d["latency_ms"] for d in calls])) if calls else float("nan"),
            "model_fallbacks": sum(1 for d in mine if d.get("event") == "decision" and d.get("status") == "FALLBACK"),
            "jev_applied": sum(1 for d in mine if d.get("event") == "apply" and d.get("mode") == "jev"),
            "jev_vetoes": sum(1 for d in mine if d.get("event") == "apply" and d.get("mode") == "veto"),
        })
    return out


def timeline_chart(scenario, runs, out_path):
    title = runs[0][0]["title"]
    dur, warm = runs[0][0]["duration"], runs[0][0]["warmup"]
    fig, axes = plt.subplots(5, 1, figsize=(9, 10.5), sharex=True,
                             gridspec_kw={"height_ratios": [1, 1.3, 1, 1.4, 1.2]})
    ax_load, ax_par, ax_tot, ax_lat, ax_lag = axes
    for i, (meta, samples, latency, load, _) in enumerate(runs):
        alpha = 0.95 if i == 0 else 0.45
        if i == 0:
            ax_load.plot(load.t, load.target_rate, color=MUTED)
            cap = 1e6 / load.agg_cost_us
            if cap.nunique() > 1:
                ax_load.plot(load.t, cap, color=MUTED, linestyle=":", linewidth=1.2)
                ax_load.annotate("capacity of one Aggregate subtask (rec/s)", (load.t.iloc[-1], cap.iloc[-1]),
                                 ha="right", va="bottom", color=MUTED, fontsize=8)
            if load.hot_fraction.max() > 0:
                start = load.t[load.hot_fraction > 0].min()
                ax_load.axvspan(start, load.t.max(), color=GRID, alpha=0.6, linewidth=0)
                ax_load.annotate("60% of records on 4 hot keys", (start + 3, load.target_rate.max()),
                                 va="top", color=MUTED, fontsize=8)
        for job in JOBS:
            s = samples[samples.job == job]
            sec = per_second(latency, job, dur)
            label = LABEL[job] if i == 0 else None
            ax_par.step(s.t, s.p_aggregate.ffill(), where="post", color=COLOR[job], alpha=alpha, label=label)
            ax_tot.step(s.t, s[[f"p_{v}" for v in VERTICES]].sum(axis=1, min_count=3), where="post",
                        color=COLOR[job], alpha=alpha)
            ax_lat.plot(sec.index, sec.p95.clip(lower=1), color=COLOR[job], alpha=alpha)
            lag = (s.end_offset - s.committed_offset).clip(lower=0)
            ax_lag.plot(s.t, lag, color=COLOR[job], alpha=alpha)
    ax_load.set_title(f"{title}\nInput rate sent to each job (records/s)")
    ax_par.set_title("Parallelism of the Aggregate vertex (the bottleneck)")
    ax_tot.set_title("Total parallelism of all three vertices (resource use)")
    ax_lat.set_title("p95 latency per second (ms, log scale)")
    ax_lat.set_yscale("log")
    ax_lat.axhline(SLO_MS, color=MUTED, linestyle="--", linewidth=1)
    ax_lat.annotate(f"{SLO_MS} ms limit", (dur, SLO_MS), ha="right", va="bottom", color=MUTED, fontsize=8)
    ax_lag.set_title("Backlog: Kafka consumer lag (records; offsets are committed at each 10 s checkpoint)")
    ax_lag.set_xlabel("Seconds since the measured phase started (negative = warm-up)")
    for ax in axes:
        ax.axvline(0, color=MUTED, linewidth=0.8)
        ax.set_xlim(-warm, dur)
        ax.set_ylim(bottom=None if ax is ax_lat else 0)
    ax_par.legend(loc="upper left", ncol=len(JOBS))
    fig.text(0.99, 0.005, f"{len(runs)} repetition(s); the first is drawn darker", ha="right", color=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


SUMMARY_PANELS = [
    ("latency_p95_ms", "p95 latency (ms)"),
    ("slo_violation_s", f"Seconds with p95 above {SLO_MS} ms"),
    ("recovery_s", "Recovery time after workload changes (s)"),
    ("lag_max_records", "Largest backlog (records)"),
    ("parallelism_total_mean", "Average total parallelism"),
    ("scaling_actions", "Scaling actions"),
]


def summary_chart(df, out_path):
    scenarios = list(dict.fromkeys(df.scenario))
    fig, axes = plt.subplots(2, 3, figsize=(13, 6.6))
    x = np.arange(len(scenarios))
    for ax, (col, title) in zip(axes.ravel(), SUMMARY_PANELS):
        for k, job in enumerate(JOBS):
            g = df[df.job == job].groupby("scenario")[col]
            mean = g.mean().reindex(scenarios)
            lo, hi = g.min().reindex(scenarios), g.max().reindex(scenarios)
            step = 0.8 / len(JOBS)
            pos = x + (k - (len(JOBS) - 1) / 2) * step
            ax.bar(pos, mean, width=step - 0.04, color=COLOR[job], label=LABEL[job] if col == SUMMARY_PANELS[0][0] else None)
            ax.errorbar(pos, mean, yerr=[mean - lo, hi - mean], fmt="none", ecolor=INK, elinewidth=0.9, capsize=2)
            for p, m in zip(pos, mean):
                if not np.isnan(m):
                    ax.annotate(f"{m:,.0f}" if m >= 10 else f"{m:.1f}", (p, m), ha="center", va="bottom",
                                fontsize=7, color=MUTED, xytext=(0, 2), textcoords="offset points")
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels([s.replace("_", "-") for s in scenarios])
        ax.grid(axis="x", visible=False)
    axes[0, 0].legend(loc="upper left")
    fig.text(0.99, 0.005, "Bars: mean over repetitions. Whiskers: lowest and highest repetition.",
             ha="right", color=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def fmt(v):
    if isinstance(v, float) and np.isnan(v):
        return "-"
    if abs(v) >= 100:
        return f"{v:,.0f}"
    if abs(v) >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}".rstrip("0").rstrip(".")


def summary_markdown(df, out_path):
    rows = [
        ("latency_p50_ms", "p50 latency (ms)"), ("latency_p95_ms", "p95 latency (ms)"),
        ("latency_p99_ms", "p99 latency (ms)"), ("slo_violation_s", f"seconds with p95 > {SLO_MS} ms"),
        ("recovery_s", "recovery time (s)"), ("throughput_rps", "throughput (rec/s)"),
        ("lag_max_records", "largest backlog (records)"), ("lag_mean_records", "average backlog (records)"),
        ("parallelism_total_mean", "average total parallelism"), ("parallelism_aggregate_max", "peak Aggregate parallelism"),
        ("tm_pods_mean", "average TaskManager pods"), ("scaling_actions", "scaling actions"),
        ("direction_reversals", "direction reversals"),
    ]
    lines = ["# Benchmark summary", "", "Each cell is the mean over repetitions, with the lowest and highest repetition in brackets.", ""]
    for scenario in dict.fromkeys(df.scenario):
        d = df[df.scenario == scenario]
        lines += [f"## {scenario} ({d.rep.nunique()} repetitions)", "",
                  "| Metric | " + " | ".join(SHORT[j] for j in JOBS) + " |", "|---|" + "---|" * len(JOBS)]
        for col, name in rows:
            cells = []
            for job in JOBS:
                v = d[d.job == job][col]
                cells.append(f"{fmt(v.mean())} ({fmt(v.min())}–{fmt(v.max())})")
            lines.append(f"| {name} | " + " | ".join(cells) + " |")
        lines.append("")
        for job in [j for j in JOBS if has_model(j)]:
            j = d[d.job == job]
            lines += [f"{SHORT[job]} model use per run: {fmt(j.model_calls.mean())} calls, {fmt(j.model_input_tokens.mean())} input and "
                      f"{fmt(j.model_output_tokens.mean())} output tokens, "
                      f"${j.model_cost_usd.mean():.5f} (${j.model_cost_usd_per_hour.mean():.4f} per hour), "
                      f"mean call latency {fmt(j.model_latency_ms_mean.mean())} ms, {int(j.model_fallbacks.sum())} fallbacks in total.", ""]
    open(out_path, "w").write("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results"))
    args = ap.parse_args()
    charts = os.path.join(args.results, "charts")
    os.makedirs(charts, exist_ok=True)

    order = ["ramp", "spike", "scale_down", "skew", "capacity"]
    found = sorted({os.path.basename(os.path.dirname(os.path.dirname(p)))
                    for p in glob.glob(os.path.join(args.results, "*", "rep*", "meta.json"))},
                   key=lambda s: order.index(s) if s in order else 99)
    global JOBS
    metas = [json.load(open(p)) for p in glob.glob(os.path.join(args.results, "*", "rep*", "meta.json"))]
    if metas:
        JOBS = tuple(metas[0].get("jobs", ["builtin", "jev"]))
    records = []
    for scenario in found:
        runs = [load_run(os.path.dirname(p))
                for p in sorted(glob.glob(os.path.join(args.results, scenario, "rep*", "meta.json")))]
        timeline_chart(scenario, runs, os.path.join(charts, f"{scenario}.png"))
        for run in runs:
            for job in JOBS:
                records.append({"scenario": scenario, "rep": run[0]["rep"], "job": job, **metrics(*run, job)})
    if not records:
        raise SystemExit(f"no finished runs under {args.results}")
    df = pd.DataFrame(records)
    df.to_csv(os.path.join(args.results, "summary.csv"), index=False)
    summary_markdown(df, os.path.join(args.results, "summary.md"))
    summary_chart(df, os.path.join(charts, "summary.png"))
    print(open(os.path.join(args.results, "summary.md")).read())


if __name__ == "__main__":
    main()
