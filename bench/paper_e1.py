#!/usr/bin/env python3
"""Analysis of the open-loop backlog-signal study (bench/e1.sh).

For every run and job it compares Flink's pendingRecords with the true backlog (broker end offsets
minus the reader's position) while the job is overloaded, and sets the measured share next to the
prediction of the consumer model in paper/research/fetch_lag_sim.py.

  python3 bench/paper_e1.py [--root results-e1] [--out paper]

Writes <root>/e1.csv, <out>/gen/tab_e1.tex, <out>/gen/numbers_e1.tex, <out>/figs/e1_series.pdf,
<out>/figs/e1_model.pdf and prints a digest.
"""
import argparse
import glob
import importlib.util
import json
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "bench"))
import analyze  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402

INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
FETCH_COLOR = {4096: "#1baf7a", 65536: "#eda100", 262144: "#eb6834", 1048576: "#2a78d6"}
FETCH_NAME = {4096: "4 KiB", 65536: "64 KiB", 262144: "256 KiB", 1048576: "1 MiB (default)"}
CAPACITY = 1000.0  # records/s of the fixed job (Aggregate at 2 subtasks)
WIRE_OVERHEAD = 12  # bytes per record on top of the value, with the load generator's batching

plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.edgecolor": MUTED, "axes.labelcolor": INK, "xtick.color": INK, "ytick.color": INK, "text.color": INK,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.5, "axes.spines.top": False,
    "axes.spines.right": False, "font.size": 7.5, "axes.titlesize": 8, "axes.labelsize": 7.5,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7, "legend.frameon": False,
    "lines.linewidth": 1.2, "pdf.fonttype": 42, "font.family": "DejaVu Sans",
})



def share(v):
    """A share as a percentage: whole numbers, and one decimal below 1% so that a small share is not
    printed as zero."""
    return f"{100 * v:.1f}" if 0 < 100 * v < 0.95 else f"{100 * v:.0f}"

def simulator():
    path = os.path.join(ROOT, "paper", "research", "fetch_lag_sim.py")
    spec = importlib.util.spec_from_file_location("fetch_lag_sim", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def options(job):
    out = {}
    for o in job.split("+")[1:]:
        m = re.fullmatch(r"([a-z]+?)(\d+)", o)
        out[m.group(1) if m else o] = int(m.group(2)) if m else True
    return out


def predicted(sim, partitions_per_reader, readers, fetch_bytes, record_bytes, arrival, read, start, end, handover=3,
              node_gate=False):
    """What the model gives for one reader with the measured arrival and read rates: the share of the
    true backlog that the summed gauge shows (both averaged over the overload, which does not depend
    on where in its fetch cycle each reader is) and the backlog it does not show at the end of the
    overload, summed over the readers. handover=0 leaves out the batches Flink's reader holds."""
    f = max(1, int(fetch_bytes / (record_bytes + WIRE_OVERHEAD)))
    lam = lambda t: (arrival if start <= t < end else 400.0) / readers  # noqa: E731
    rows = sim.run(partitions_per_reader, f, 500, lam, lambda t: read / readers, end, sample_every=1.0, handover=handover,
                   node_gate=node_gate)
    w = [(lag, g) for t, lag, g, *_ in rows if start + 20 <= t <= end and lag > 0]
    tail = [lag - g for t, lag, g, *_ in rows if end - 30 <= t <= end]
    ratio = sum(g for _, g in w) / sum(lag for lag, _ in w) if w else float("nan")
    return ratio, (float(np.mean(tail)) * readers if tail else float("nan"))


def collect(root):
    sim = simulator()
    records, runs = [], {}
    for meta_path in sorted(glob.glob(os.path.join(root, "*", "*", "rep*", "meta.json"))):
        path = os.path.dirname(meta_path)
        config = os.path.relpath(path, root).split(os.sep)[0]
        meta = json.load(open(meta_path))
        samples = pd.read_csv(os.path.join(path, "samples.csv"))
        latency = pd.read_csv(os.path.join(path, "latency.csv"))
        load = pd.read_csv(os.path.join(path, "load.csv"))
        start, end = meta["events"][0], meta["events"][1]
        rho = SCENARIOS[meta["scenario"]].rate(start + 1) / CAPACITY
        record = max(40, meta.get("record_bytes") or 0)
        partitions = meta.get("partitions", 8)
        runs[(config, meta["scenario"], meta["rep"])] = (meta, samples, latency, load)
        for job in meta["jobs"]:
            o = options(job)
            fetch = o.get("f", 1048576)
            readers = o.get("sp", 1)
            s = samples[samples.job == job].copy()
            # The position gauge is read through Flink's REST API and is older than the end offsets,
            # which come straight from the broker: compare it with the end offsets of that time.
            stale = analyze.reader_staleness(s, meta)
            s["true_lag"] = (np.interp(s.t - stale, s.t, s.end_offset) - s.position_offset).clip(lower=0)
            o_w = s[(s.t >= start + 20) & (s.t <= end)]
            span = o_w.t.iloc[-1] - o_w.t.iloc[0] if len(o_w) > 1 else np.nan
            arrival = (o_w.end_offset.iloc[-1] - o_w.end_offset.iloc[0]) / span if len(o_w) > 1 else np.nan
            read = (o_w.position_offset.iloc[-1] - o_w.position_offset.iloc[0]) / span if len(o_w) > 1 else np.nan
            model_ratio, model_unseen = predicted(sim, partitions // readers, readers, fetch, record, arrival, read, start, end)
            consumer_ratio, _ = predicted(sim, partitions // readers, readers, fetch, record, arrival, read, start, end, handover=0)
            # The same run under the fetch rule of Kafka clients from 4.0 on (the benchmark ran 3.4.0).
            newer_ratio, newer_unseen = predicted(sim, partitions // readers, readers, fetch, record, arrival, read, start, end,
                                                  node_gate=True)
            w = s[(s.t >= start + 20) & (s.t <= end) & (s.true_lag > 0)]
            tail = s[(s.t >= end - 30) & (s.t <= end)]
            calm = latency[(latency.job == job) & (latency.t >= 0) & (latency.t < start - 5)]
            sent = load[(load.t >= start + 20) & (load.t <= end)]
            records.append({
                "config": config, "scenario": meta["scenario"], "rep": meta["rep"], "job": job,
                "partitions": partitions, "readers": readers, "partitions_per_reader": partitions // readers,
                "record_bytes": record, "rho": rho, "fetch_bytes": fetch, "staleness_s": stale,
                "arrival_rate": arrival, "rho_measured": arrival / read if read else np.nan,
                "delivered_ok": bool(arrival >= 0.95 * rho * CAPACITY),
                # Share of the backlog the metric shows, both averaged over the overload.
                "seen_ratio": float(w.pending_records.mean() / w.true_lag.mean()) if len(w) else np.nan,
                "seen_ratio_median": float((w.pending_records / w.true_lag).median()) if len(w) else np.nan,
                "seen_ratio_end": float(tail.pending_records.mean() / tail.true_lag.mean()) if len(tail) else np.nan,
                "unseen_end": float((tail.true_lag - tail.pending_records).mean()) if len(tail) else np.nan,
                "true_lag_max": float(s.true_lag.max()),
                "read_rate": read,
                "fetch_rate": float(w.fetch_rate.median()) if len(w) and "fetch_rate" in w else np.nan,
                "calm_p95_ms": float(calm.p95_ms.median()) if len(calm) else np.nan,
                "sent_rate": float((sent.sent_total.iloc[-1] - sent.sent_total.iloc[0]) / (sent.t.iloc[-1] - sent.t.iloc[0])) if len(sent) > 1 else np.nan,
                "predicted_ratio": model_ratio, "predicted_unseen_end": model_unseen,
                "predicted_ratio_consumer_only": consumer_ratio,
                "predicted_ratio_newer_client": newer_ratio, "predicted_unseen_newer_client": newer_unseen,
            })
    df = pd.DataFrame(records)
    if len(df):
        df.to_csv(os.path.join(root, "e1.csv"), index=False)
        short = df[~df.delivered_ok]
        if len(short):
            print(f"excluded {short.groupby(['config', 'scenario', 'rep']).ngroups} run(s) in which the load generator fell behind")
        df = df[df.delivered_ok]
    return df, runs


def config_label(row):
    return f"{row.partitions_per_reader} per reader, {row.record_bytes} B, $\\rho$={row.rho:g}"


def table(df, path, numbers):
    fetches = sorted(df.fetch_bytes.unique(), reverse=True)
    lines = ["\\begin{tabular}{@{}rlrr" + "r" * len(fetches) + "@{}}", "\\toprule",
             "Part./reader & Record & $\\rho$ & $n$ & " + " & ".join(FETCH_NAME[f].replace(" (default)", "") for f in fetches) + " \\\\",
             "\\midrule"]
    keys = df.groupby(["partitions_per_reader", "record_bytes", "rho"], sort=False).size().index
    order = sorted(keys, key=lambda k: (k[2] != 2.0, -k[0], k[1], k[2]))
    for ppr, rec, rho in order:
        d = df[(df.partitions_per_reader == ppr) & (df.record_bytes == rec) & (df.rho == rho)]
        cells = []
        for f in fetches:
            x = d[d.fetch_bytes == f]
            if not len(x):
                cells.append("--")
                continue
            m, p = x.seen_ratio.mean(), x.predicted_ratio.mean()
            tag = f"P{ppr}R{rec}Rho{int(rho * 10)}F{f}"
            numbers.append(f"\\newcommand{{\\Seen{macro(tag)}}}{{{100 * m:.0f}}}")
            cells.append(f"{share(m)}\\% {{\\scriptsize ({share(p)})}}")
        runs = d.groupby(["config", "scenario", "rep"]).ngroups
        size = f"{rec} B" if rec < 1024 else f"{rec // 1024} KiB"
        lines.append(f"{ppr} & {size} & {rho:g} & {runs} & " + " & ".join(cells) + " \\\\")
    lines += ["\\bottomrule", "\\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


HIGH_RATE = "p8-r1024"  # one job alone, 1,024-byte records: a reader that moves about 1 MB/s


def named(df, numbers):
    """The numbers the text quotes, under readable names."""
    def cell(ppr, rec, rho, fetch, col):
        d = df[(df.partitions_per_reader == ppr) & (df.record_bytes == rec) & (df.rho == rho) & (df.fetch_bytes == fetch)]
        return float(d[col].mean()) if len(d) else None

    def add(name, value, fmt):
        if value is not None and not np.isnan(value):
            numbers.append(f"\\newcommand{{\\{name}}}{{{fmt(value)}}}")

    pct = share
    thousands = lambda v: f"{v / 1000:,.0f}"  # noqa: E731
    for tag, fetch in (("Default", 1048576), ("Quarter", 262144), ("SixtyFour", 65536), ("Four", 4096)):
        add(f"eoneSeen{tag}", cell(8, 40, 2.0, fetch, "seen_ratio"), pct)
        add(f"eoneModel{tag}", cell(8, 40, 2.0, fetch, "predicted_ratio"), pct)
        add(f"eoneUnseen{tag}", cell(8, 40, 2.0, fetch, "unseen_end"), thousands)
        add(f"eoneModelUnseen{tag}", cell(8, 40, 2.0, fetch, "predicted_unseen_end"), thousands)
        add(f"eoneSeenOnePart{tag}", cell(1, 40, 2.0, fetch, "seen_ratio"), pct)
        add(f"eoneModelOnePart{tag}", cell(1, 40, 2.0, fetch, "predicted_ratio"), pct)
        add(f"eoneSeenWide{tag}", cell(8, 256, 2.0, fetch, "seen_ratio"), pct)
        if tag != "Default":  # the default is added below, for all partition counts
            add(f"eoneSeenFourPart{tag}", cell(4, 40, 2.0, fetch, "seen_ratio"), pct)
            add(f"eoneSeenMany{tag}", cell(64, 40, 2.0, fetch, "seen_ratio"), pct)
        add(f"eoneFetchRate{tag}", cell(8, 40, 2.0, fetch, "fetch_rate"), lambda v: f"{v:.1f}")
        add(f"eoneCalm{tag}", cell(8, 40, 2.0, fetch, "calm_p95_ms"), lambda v: f"{v:.0f}")
        add(f"eoneRead{tag}", cell(8, 40, 2.0, fetch, "read_rate"), lambda v: f"{v:,.0f}")
    add("eoneBacklogMax", cell(8, 40, 2.0, 1048576, "true_lag_max"), thousands)
    # How far the model is from the measurement, over all cells of the grid (percentage points).
    # The cell added after review (HIGH_RATE) is not part of the grid: its prediction was written
    # before it ran, and it is reported next to that prediction.
    grid = df[df.config != HIGH_RATE]
    g = grid.groupby(["config", "scenario", "fetch_bytes"])[["seen_ratio", "predicted_ratio", "predicted_ratio_consumer_only"]].mean()
    g = g.dropna()  # a cell without a measurement is not counted
    add("eoneCells", float(len(g)), lambda v: f"{v:.0f}")
    # With the sampler's refresh the positions and the end offsets of a sample are almost of the same
    # time; the paper quotes the largest difference over all jobs.
    add("eoneStaleness", float(df.staleness_s.abs().max()), lambda v: f"{v:.1f}")
    add("eoneModelGapMax", float(np.ceil(100 * (g.seen_ratio - g.predicted_ratio).abs().max() - 1e-9)) / 100, pct)
    # In how many of the cells that are off by 3 points or more the model is above the measurement.
    off = g[(g.seen_ratio - g.predicted_ratio).abs() >= 0.03]
    add("eoneModelOffCells", float(len(off)), lambda v: f"{v:.0f}")
    add("eoneModelOffAbove", float((off.predicted_ratio > off.seen_ratio).sum()), lambda v: f"{v:.0f}")
    # The model under the fetch rule of clients from 4.0 on, for the cells the text names.
    for tag, fetch in (("Default", 1048576), ("Quarter", 262144)):
        add(f"eoneNewer{tag}", cell(8, 40, 2.0, fetch, "predicted_ratio_newer_client"), pct)
        add(f"eoneNewerUnseen{tag}", cell(8, 40, 2.0, fetch, "predicted_unseen_newer_client"), thousands)
    add("eoneNewerWideDefault", cell(8, 256, 2.0, 1048576, "predicted_ratio_newer_client"), pct)
    add("eoneModelWideDefault", cell(8, 256, 2.0, 1048576, "predicted_ratio"), pct)
    # Runs per configuration, for the text.
    counts = grid.groupby("config").apply(lambda x: x.groupby(["scenario", "rep"]).ngroups)
    high = df[df.config == HIGH_RATE]
    if len(high):
        add("eoneRunsHighRate", float(high.groupby(["scenario", "rep"]).ngroups), lambda v: f"{v:.0f}")
        for name, col, fmt in (("Seen", "seen_ratio", pct), ("Model", "predicted_ratio", pct),
                               ("Unseen", "unseen_end", thousands), ("ModelUnseen", "predicted_unseen_end", thousands),
                               ("Read", "read_rate", lambda v: f"{v:,.0f}"), ("BacklogMax", "true_lag_max", thousands),
                               ("ModelNewer", "predicted_ratio_newer_client", pct)):
            if col in high:
                add(f"eone{name}HighRate", float(high[col].mean()), fmt)
        add("eoneSeenHighRateMin", float(high.seen_ratio.min()), pct)
        add("eoneSeenHighRateMax", float(high.seen_ratio.max()), pct)
    add("eoneRunsMin", float(counts.min()), lambda v: f"{v:.0f}")
    add("eoneRunsMax", float(counts.max()), lambda v: f"{v:.0f}")
    add("eoneModelGapMean", float((g.seen_ratio - g.predicted_ratio).abs().mean()), pct)
    add("eoneConsumerGapMax", float((g.seen_ratio - g.predicted_ratio_consumer_only).abs().max()), pct)
    add("eoneConsumerGapMean", float((g.seen_ratio - g.predicted_ratio_consumer_only).abs().mean()), pct)
    add("eoneSeenManyDefault", cell(64, 40, 2.0, 1048576, "seen_ratio"), pct)
    add("eoneSeenFourPartDefault", cell(4, 40, 2.0, 1048576, "seen_ratio"), pct)


def macro(tag):
    words = {"0": "Zero", "1": "One", "2": "Two", "3": "Three", "4": "Four", "5": "Five", "6": "Six", "7": "Seven",
             "8": "Eight", "9": "Nine"}
    return "".join(words.get(c, c) for c in tag)


def fig_series(runs, path, numbers, config="p8-r40", scenario="regime20"):
    keys = sorted(k for k in runs if k[0] == config and k[1] == scenario)
    if not keys:
        return
    meta, samples, latency, load = runs[keys[0]]
    # The highest value the metric reaches with the default fetch size in the run shown.
    default = next(j for j in meta["jobs"] if "f" not in options(j))
    peak = samples[(samples.job == default) & (samples.t >= 0)].pending_records.max()
    numbers.append(f"\\newcommand{{\\eoneSawtoothMax}}{{{peak / 1000:,.0f}}}")
    fig, ax = plt.subplots(figsize=(3.4, 2.1))
    first = True
    for job in sorted(meta["jobs"], key=lambda j: -options(j).get("f", 1048576)):
        f = options(job).get("f", 1048576)
        s = samples[samples.job == job]
        if first:
            ax.plot(s.t, (s.end_offset - s.position_offset) / 1000, color=INK, lw=1.6, label="true backlog")
            first = False
        ax.plot(s.t, s.pending_records / 1000, color=FETCH_COLOR[f], label=f"metric, fetch size {FETCH_NAME[f]}")
    ax.axvspan(meta["events"][0], meta["events"][1], color=GRID, alpha=0.5, lw=0)
    ax.set_xlim(0, meta["duration"])
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Seconds (shaded: load is twice the job's capacity)")
    ax.set_ylabel("Thousand records")
    ax.legend(loc="upper left", handlelength=1.2, fontsize=6.3, borderaxespad=0.1, labelspacing=0.25)
    fig.tight_layout(pad=0.3)
    fig.savefig(path)
    plt.close(fig)


def fig_model(df, path):
    fig, ax = plt.subplots(figsize=(3.0, 2.6))
    g = df.groupby(["config", "scenario", "fetch_bytes"], as_index=False)[["seen_ratio", "predicted_ratio"]].mean()
    for f, d in g.groupby("fetch_bytes"):
        ax.plot(d.predicted_ratio, d.seen_ratio, "o", ms=4, color=FETCH_COLOR[f], label=FETCH_NAME[f])
    ax.plot([0, 1], [0, 1], color=MUTED, lw=0.7, ls="--")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Share of the backlog seen, model")
    ax.set_ylabel("Share of the backlog seen, measured")
    ax.legend(loc="upper left", title="Fetch size")
    fig.tight_layout(pad=0.3)
    fig.savefig(path)
    plt.close(fig)


def high_rate_demand(root, numbers):
    """The cell at about 1 MB/s per reader: what the rule would compute as demand on a one-minute
    window (records read plus the growth of the reported backlog) against the true arrival rate, by
    how long after the start of the overload the window begins; and the unseen backlog once it has
    stopped growing. Means over the runs of the cell."""
    ratios, unseen = {}, []
    for d in sorted(glob.glob(os.path.join(root, HIGH_RATE, "*", "rep*"))):
        if not os.path.exists(os.path.join(d, "meta.json")):
            continue
        meta = json.load(open(os.path.join(d, "meta.json")))
        s = pd.read_csv(os.path.join(d, "samples.csv"))
        s = s[s.job == meta["jobs"][0]].dropna(subset=["position_offset", "pending_records"]).sort_values("t")
        t = s.t.to_numpy(float)
        at = lambda col, x: float(np.interp(x, t, s[col].to_numpy(float)))  # noqa: E731
        start, end = meta["events"][:2]
        for lead in range(0, int(end - start) - 59, 10):
            a, b = start + lead, start + lead + 60
            arrived = at("end_offset", b) - at("end_offset", a)
            believed = at("position_offset", b) - at("position_offset", a) + at("pending_records", b) - at("pending_records", a)
            ratios.setdefault(lead, []).append(believed / arrived)
        late = s[(s.t >= start + 40) & (s.t <= end)]
        unseen.append(float((late.end_offset - late.position_offset - late.pending_records).mean()))
    if not ratios:
        return
    mean = {lead: float(np.mean(v)) for lead, v in ratios.items()}
    later = [x for lead, v in ratios.items() if lead >= 30 for x in v]
    numbers.append(f"\\newcommand{{\\eoneDemandHighRateFirst}}{{{100 * mean[0]:.0f}}}")
    numbers.append(f"\\newcommand{{\\eoneDemandHighRateTen}}{{{100 * mean[10]:.0f}}}")
    numbers.append(f"\\newcommand{{\\eoneDemandHighRateTwenty}}{{{100 * mean[20]:.0f}}}")
    numbers.append(f"\\newcommand{{\\eoneDemandHighRateLaterOff}}{{{100 * max(abs(x - 1) for x in later):.0f}}}")
    numbers.append(f"\\newcommand{{\\eoneUnseenConstHighRate}}{{{np.mean(unseen) / 1000:.0f}}}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(ROOT, "results-e1"))
    ap.add_argument("--out", default=os.path.join(ROOT, "paper"))
    args = ap.parse_args()
    df, runs = collect(args.root)
    if not len(df):
        raise SystemExit(f"no finished runs under {args.root}")
    gen, figs = os.path.join(args.out, "gen"), os.path.join(args.out, "figs")
    os.makedirs(gen, exist_ok=True)
    os.makedirs(figs, exist_ok=True)
    numbers = []
    table(df, os.path.join(gen, "tab_e1.tex"), numbers)
    named(df, numbers)
    high_rate_demand(args.root, numbers)
    # The grid alone: the cell at about 1 MB/s is outside it and has its own count.
    numbers.append(f"\\newcommand{{\\EOneRuns}}{{{df[df.config != HIGH_RATE].groupby(['config', 'scenario', 'rep']).ngroups}}}")
    fig_series(runs, os.path.join(figs, "e1_series.pdf"), numbers)
    open(os.path.join(gen, "numbers_e1.tex"), "w").write("% Generated by bench/paper_e1.py. Do not edit.\n" + "\n".join(sorted(set(numbers))) + "\n")
    fig_model(df, os.path.join(figs, "e1_model.pdf"))
    pd.set_option("display.width", 220)
    g = df.groupby(["config", "scenario", "fetch_bytes"]).agg(
        n=("rep", "nunique"), seen=("seen_ratio", "mean"), seen_min=("seen_ratio", "min"), seen_max=("seen_ratio", "max"),
        model=("predicted_ratio", "mean"), consumer=("predicted_ratio_consumer_only", "mean"), seen_end=("seen_ratio_end", "mean"), unseen_end=("unseen_end", "mean"),
        model_unseen=("predicted_unseen_end", "mean"), rho=("rho_measured", "mean"),
        lag_max=("true_lag_max", "mean"), read=("read_rate", "mean"), fetches=("fetch_rate", "mean"),
        calm_p95=("calm_p95_ms", "mean"), stale=("staleness_s", "mean"))
    print(g.round(3).to_string())


if __name__ == "__main__":
    main()
