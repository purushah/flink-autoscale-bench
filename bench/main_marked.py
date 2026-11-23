#!/usr/bin/env python3
"""Main campaign: the planned pooled contrasts once more, for the checks the analysis plan asks for.

    python3 bench/paper.py main                                   # first: writes results-v3/runs.csv
    python3 bench/main_marked.py [--tex paper/gen/numbers_main_marked.tex]

For both arms, the three planned contrasts (PaC - Builtin, DM - PaC, LLM - PaC) on the two primary
metrics (seconds over the limit, slots):
 - with all runs, and without the marked runs (plan entry of 2026-11-22 01:16 PST: the seven runs with
   a job exception and the run in which a TaskManager was lost);
 - for the runs made by day and by night (amendment of 2026-11-21 13:27 PST; a check, not planned);
 - next to each pooled figure the number of pairs whose divisor is the floor;
 - the verdict against the margins of practical relevance (10% of the seconds, 4% of the slots);
 - the differences per scenario in their own units, and which of their intervals exclude zero.
It uses the functions of paper.py, so a figure printed here for all runs is the one in
paper/gen/summary_main.txt. Holm's correction is over the six tests of an arm, as in the plan.
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze  # noqa: E402
import paper  # noqa: E402

# (arm, scenario, repetition): what happened. None of these events lies inside a load change.
MARKED = {
    ("accurate", "capacity", 2): "job exception in the warm-up",
    ("default", "scale_down", 2): "job exception in the warm-up",
    ("accurate", "scale_down", 3): "job exception in the warm-up; not settled at the start",
    ("default", "spike", 2): "job exception 128 s after the load change",
    ("accurate", "ramp", 3): "job exception 247 s after the load change",
    ("accurate", "skew", 3): "job exception 159 s after the load change; not settled at the start",
    ("accurate", "ramp", 4): "job exception 42 s after the load change",
    ("accurate", "spike", 3): "a TaskManager of the LLM job lost 72 s after the load change",
}
MARGIN = {"slo_violation_s": 0.10, "slots_mean": 0.04}
SHORT = {"slo_violation_s": "slo", "slots_mean": "slots"}
# The VM was stopped at 20:28Z on 19 Nov. Every run started before that was made by day.
DAY_END = "2026-11-19T21:00:00Z"


def run_keys(df):
    return [tuple(k) for k in df[["arm", "scenario", "rep"]].itertuples(index=False, name=None)]


def day_runs(root):
    out = set()
    for arm, scenario, rep in sorted(set(run_keys(pd.read_csv(os.path.join(root, "runs.csv"))))):
        meta = json.load(open(os.path.join(root, arm, scenario, f"rep{rep}", "meta.json")))
        if meta["started_utc"] < DAY_END:
            out.add((arm, scenario, rep))
    return out


def start_state(root, keys):
    """Per run, from files that hold no outcome: by how much the warm-up ran over its plan, and the
    Aggregate parallelism of each job at the start of the measured phase."""
    out = {}
    for arm, scenario, rep in sorted(keys):
        d = os.path.join(root, arm, scenario, f"rep{rep}")
        meta = json.load(open(os.path.join(d, "meta.json")))
        sizes = {}
        s = pd.read_csv(os.path.join(d, "samples.csv"), usecols=["t", "job", "p_aggregate"])
        s = s[(s.t >= 0) & s.p_aggregate.notna()]
        for job, part in s.groupby("job"):
            sizes[job] = int(part.sort_values("t").p_aggregate.iloc[0])
        out[(arm, scenario, rep)] = (meta["warmup"] - meta["warmup_planned"], sizes)
    return out


def disturbed_start(state):
    """A rule that uses no outcome (review round 3): the warm-up ran more than 30 s over its plan, or
    the jobs did not start the measured phase at one size."""
    out = {}
    for key, (over, sizes) in state.items():
        why = []
        if over > 30:
            why.append(f"warm-up {over:.0f} s over its plan")
        if len(set(sizes.values())) > 1:
            why.append("jobs at unequal sizes at the start (" + ", ".join(f"{paper.NAME.get(j, j)} {p}" for j, p in sorted(sizes.items())) + ")")
        if why:
            out[key] = "; ".join(why)
    return out


def seconds_by_phase(root, keys):
    """Per run and job: seconds over the limit before the first load change and from it on, counted as
    analyze.metrics counts them (a second without a healthy p95 counts as over)."""
    rows = []
    for arm, scenario, rep in sorted(keys):
        meta, samples, latency, load, decisions = analyze.load_run(os.path.join(root, arm, scenario, f"rep{rep}"))
        dur, first = meta["duration"], (meta["events"][0] if meta["events"] else 0.0)
        for job in meta["jobs"]:
            sec = analyze.per_second(latency, job, dur)
            healthy = analyze.healthy_series(sec.p95).loc[0:dur]
            over = healthy.isna() | (healthy > analyze.SLO_MS)
            t = over.index.to_numpy(float)
            rows.append({"arm": arm, "scenario": scenario, "rep": rep, "job": job,
                         "slo_before_s": int(over[t < first].sum()), "slo_after_s": int(over[t >= first].sum())})
    return pd.DataFrame(rows)


def relative_diffs(df, arm, a, b, col):
    """The paired differences of paper.pooled(), each divided by its scenario's level (with the floor)."""
    diffs = []
    for sc in paper.present(df, df.arm == arm):
        d = df[(df.arm == arm) & (df.scenario == sc)].pivot(index="rep", columns="job", values=col)
        if a not in d or b not in d:
            continue
        x = (d[a] - d[b]).dropna().to_numpy(float)
        scale = max(np.nanmean(np.r_[d[a].to_numpy(float), d[b].to_numpy(float)]), paper.POOL_FLOOR.get(col, 0.0))
        if len(x) and scale > 0:
            diffs += list(x / scale)
    return np.array(diffs)


def raw_diffs(df, arm, a, b, col):
    """a minus b per run of an arm, in the metric's own unit."""
    d = df[df.arm == arm].pivot_table(index=["scenario", "rep"], columns="job", values=col)
    return (d[a] - d[b]).dropna().to_numpy(float)


def at_floor(df, arm, a, b, col):
    """How many pairs are divided by the floor instead of the scenario's mean, and in which scenarios."""
    n, where = 0, []
    for sc in paper.present(df, df.arm == arm):
        d = df[(df.arm == arm) & (df.scenario == sc)].pivot(index="rep", columns="job", values=col)
        if a not in d or b not in d:
            continue
        pairs = len((d[a] - d[b]).dropna())
        level = np.nanmean(np.r_[d[a].to_numpy(float), d[b].to_numpy(float)])
        if pairs and level < paper.POOL_FLOOR.get(col, 0.0):
            n += pairs
            where.append(sc)
    return n, where


SCENARIO_LABEL = {"ramp": "Ramp", "spike": "Spike", "scale_down": "Drop", "skew": "Skew", "capacity": "Slow-down"}


def half_num(v):
    """paper.num with halves rounded up, as in the other tables (4.25 -> 4.3, not 4.2)."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "--"
    a = abs(v)
    if a >= 100:
        return f"{int(paper.half_up(v, 0)):,}"
    return paper.half_up(v, 2 if a < 1 else 1)


def means_table(df, name):
    """Mean seconds over the limit / mean slots per scenario, arm and controller (all runs)."""
    macro = {"Builtin": "\\builtin", "PaC": "\\pac", "DM": "\\dm", "LLM": "\\llm"}
    jobs = [j for j in paper.JOB_ORDER if j in set(df.job)]
    out = ["\\begin{tabular}{@{}l" + "@{\\ \\ \\ }r" * len(jobs) + "@{}}", "\\toprule",
           "Scenario & " + " & ".join(macro[name[j]] for j in jobs) + " \\\\"]
    for arm in paper.ARMS:
        if not (df.arm == arm).any():
            continue
        out.append("\\midrule")
        out.append(f"\\multicolumn{{{len(jobs) + 1}}}{{@{{}}l}}{{\\emph{{{ARM_LABEL[arm]}}}}} \\\\")
        for sc in paper.present(df, df.arm == arm):
            cells = []
            for j in jobs:
                d = df[(df.arm == arm) & (df.scenario == sc) & (df.job == j)]
                cells.append(f"{half_num(d.slo_violation_s.mean())} / {paper.half_up(d.slots_mean.mean(), 1)}")
            out.append(f"{SCENARIO_LABEL.get(sc, sc)} & " + " & ".join(cells) + " \\\\")
    out += ["\\bottomrule", "\\end{tabular}"]
    return "\n".join(out) + "\n"


def tests(df, arm):
    """(a, b, column, pooled result, Holm-adjusted p) for the six planned tests of an arm."""
    out = []
    for a, b in paper.PRIMARY_PAIRS:
        for col in paper.PRIMARY_COLS:
            r = paper.pooled(df, arm, a, b, col)
            if r:
                out.append((a, b, col, r))
    adjusted = paper.holm([t[3]["p"] for t in out]) if out else []
    return [(a, b, col, r, float(p)) for (a, b, col, r), p in zip(out, adjusted)]


def pct(v):
    return f"{100 * v:+.1f}"


def signed(v):
    """A share as a percentage with one decimal and a typeset sign."""
    return f"{100 * v:+.1f}".replace("+", "$+$").replace("-", "$-$")


def pvalue(adj):
    return f"{adj:.2f}" if adj >= 0.1 else f"{adj:.3f}" if adj >= 0.001 else "<0.001"


ARM_LABEL = {"default": "Default fetch size", "accurate": "4\\,KiB fetch"}


def pooled_table(table_rows, name):
    """The six planned tests of each arm: pooled difference, 95% interval, Holm-adjusted p."""
    out = ["\\begin{tabular}{@{}l@{\\ \\ }r@{\\,}l@{\\ \\ }r@{\\ \\ \\ }r@{\\,}l@{\\ \\ }r@{}}", "\\toprule",
           " & \\multicolumn{3}{c}{Time over the limit} & \\multicolumn{3}{c}{Slots} \\\\",
           "\\cmidrule(lr){2-4}\\cmidrule(l){5-7}",
           "Contrast & \\multicolumn{2}{c}{\\% [95\\%]} & $p$ & \\multicolumn{2}{c}{\\% [95\\%]} & $p$ \\\\"]
    macro = {"Builtin": "\\builtin", "PaC": "\\pac", "DM": "\\dm", "LLM": "\\llm"}
    for arm in paper.ARMS:
        if arm not in table_rows:
            continue
        out.append("\\midrule")
        out.append(f"\\multicolumn{{7}}{{@{{}}l}}{{\\emph{{{ARM_LABEL[arm]}}}}} \\\\")
        for a, b in paper.PRIMARY_PAIRS:
            cells = []
            for col in paper.PRIMARY_COLS:
                r, adj = table_rows[arm][(name[a], name[b], col)]
                cells += [signed(r["mean"]), f"[{signed(r['lo'])}, {signed(r['hi'])}]", pvalue(adj)]
            out.append(f"{macro[name[a]]}{{}} $-$ {macro[name[b]]} & " + " & ".join(cells) + " \\\\")
    out += ["\\bottomrule", "\\end{tabular}"]
    return "\n".join(out) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(paper.ROOT, "results-v3"))
    ap.add_argument("--tex", default=None)
    args = ap.parse_args()
    paper.configure("main")
    df = pd.read_csv(os.path.join(args.root, "runs.csv"))
    df = df[df.job.isin(paper.JOB_ORDER)]
    name = paper.NAME
    all_runs = set(run_keys(df))
    missing = sorted(set(MARKED) - all_runs)
    if missing:
        raise SystemExit(f"marked runs that are not in runs.csv: {missing}")
    day = day_runs(args.root)
    state = start_state(args.root, all_runs)
    odd = disturbed_start(state)
    ten = set(MARKED) | set(odd)
    blocks = [("all runs", all_runs), ("without marked", all_runs - set(MARKED)), ("day", day), ("night", all_runs - day),
              ("without marked or disturbed start", all_runs - ten)]
    print(f"runs with a disturbed start (warm-up more than 30 s over its plan, or jobs at unequal sizes): {len(odd)}, "
          f"{len(set(odd) - set(MARKED))} of them not marked; {len(set(odd) & day)} made by day")
    for key, why in sorted(odd.items()):
        print(f"  {'marked  ' if key in MARKED else 'unmarked'} {key[0]}/{key[1]}/rep{key[2]}: {why}")
    print(f"runs: {len(all_runs)} in all, {len(MARKED)} marked, {len(day)} by day, {len(all_runs - day)} by night; "
          f"marked runs made by day: {len(set(MARKED) & day)} of {len(MARKED)}")
    for key, why in sorted(MARKED.items()):
        print(f"  marked {key[0]}/{key[1]}/rep{key[2]}: {why}")
    numbers = paper.Numbers()
    pooled = {}
    table_rows = {}
    for label, keep in blocks:
        part = df[[k in keep for k in run_keys(df)]]
        for arm in [a for a in paper.ARMS if (part.arm == a).any()]:
            runs = part[part.arm == arm].groupby(["scenario", "rep"]).ngroups
            print(f"=== {label}, arm {arm} ({runs} runs): pooled difference as a share of the scenario's mean")
            for a, b, col, r, adj in tests(part, arm):
                pooled[(label, arm, a, b, col)] = r
                floor_n, floor_where = at_floor(part, arm, a, b, col)
                inside = -MARGIN[col] <= r["lo"] and r["hi"] <= MARGIN[col]
                print(f"  {name[a]:4s} - {name[b]:7s} {SHORT[col]:5s}: {pct(r['mean'])}% [{pct(r['lo'])}, {pct(r['hi'])}] "
                      f"n={r['n']} p={r['p']:.4f} Holm {adj:.4f} | at the floor: {floor_n} pairs"
                      f"{' (' + ', '.join(floor_where) + ')' if floor_where else ''} | "
                      f"margin {100 * MARGIN[col]:.0f}%: {'inside' if inside else 'not inside'}")
                if label in ("without marked", "day", "night", "without marked or disturbed start"):
                    base = ("main", {"without marked": "nomark", "day": "day", "night": "night",
                                     "without marked or disturbed start": "noten"}[label], "pooled", arm,
                            name[a], "vs", name[b], SHORT[col])
                    for field in ("mean", "lo", "hi"):
                        numbers.add(paper.macro_name(*base, field),
                                    f"{100 * r[field]:+.0f}".replace("+", "$+$").replace("-", "$-$"))
                    numbers.add(paper.macro_name(*base, "n"), str(r["n"]))
                    if label in ("without marked", "without marked or disturbed start"):
                        numbers.add(paper.macro_name(*base, "inside"), "yes" if inside else "no")
                        numbers.add(paper.macro_name(*base, "p"),
                                    f"{adj:.2f}" if adj >= 0.1 else f"{adj:.3f}" if adj >= 0.001 else "<0.001")
                elif label == "all runs":
                    # The same figures as in paper/gen/numbers_main.tex, with one decimal, for the table
                    # of the pooled contrasts and the text next to it.
                    one = ("main", "all", "pooled", arm, name[a], "vs", name[b], SHORT[col])
                    for field in ("mean", "lo", "hi"):
                        numbers.add(paper.macro_name(*one, field), signed(r[field]))
                    numbers.add(paper.macro_name(*one, "p"), pvalue(adj))
                    table_rows.setdefault(arm, {})[(name[a], name[b], col)] = (r, adj)
                    # For the record: the spread per pair, the bootstrap interval the generator printed
                    # until 2026-11-22, and the difference in the metric's own unit over all runs of the arm.
                    rel = relative_diffs(part, arm, a, b, col)
                    _, blo, bhi = paper.bca_ci(rel)
                    raw = raw_diffs(part, arm, a, b, col)
                    rm, rlo, rhi = paper.boot_ci(raw)
                    print(f"       spread per pair {100 * rel.std(ddof=1):.0f}%; bootstrap interval (not used) [{pct(blo)}, {pct(bhi)}]; "
                          f"unscaled {rm:+.2f} [{rlo:+.2f}, {rhi:+.2f}] over {len(raw)} runs; lower in {int((rel < 0).sum())}, "
                          f"higher in {int((rel > 0).sum())}, equal in {int((rel == 0).sum())}")
                    numbers.add(paper.macro_name("main", "spread", arm, name[a], "vs", name[b], SHORT[col]), f"{100 * rel.std(ddof=1):.0f}")
                    unit = ("main", "unscaled", arm, name[a], "vs", name[b], SHORT[col])
                    for field, v in (("mean", rm), ("lo", rlo), ("hi", rhi)):
                        numbers.add(paper.macro_name(*unit, field), f"{v:+.1f}".replace("+", "$+$").replace("-", "$-$"))
                    base = ("main", "floor", arm, name[a], "vs", name[b], SHORT[col])
                    numbers.add(paper.macro_name(*base, "pairs"), str(floor_n))
                    numbers.add(paper.macro_name("main", "practical", arm, name[a], "vs", name[b], SHORT[col]),
                                "yes" if inside else "no")
    # Decisions of the measured phases by the plan's definition (2026-11-18): the model was asked about a
    # vertex and its most likely option is compared with the policy's choice on the same inputs.
    print("=== decisions of the measured phases: most likely option against the policy's choice")
    for job in paper.JOB_ORDER:
        d = df[df.job == job]
        if "dec_hold_n" not in d or not d.dec_hold_n.notna().any():
            continue
        asked = int(sum(d[f"dec_{k}_n"].fillna(0).sum() for k in ("hold", "up", "down")))
        agree = int(sum(d[f"dec_{k}_agree"].fillna(0).sum() for k in ("hold", "up", "down")))
        if not asked:
            continue
        print(f"  {name[job]}: {agree:,} of {asked:,}")
        numbers.add(paper.macro_name("main", "measured", name[job], "asked"), f"{asked:,}")
        numbers.add(paper.macro_name("main", "measured", name[job], "agree"), f"{agree:,}")
        numbers.add(paper.macro_name("main", "measured", name[job], "differ"), f"{asked - agree:,}")
        for cls in ("hold", "up", "down"):
            n_cls, agree_cls = int(d[f"dec_{cls}_n"].fillna(0).sum()), int(d[f"dec_{cls}_agree"].fillna(0).sum())
            print(f"      where the policy chose {cls}: {agree_cls:,} of {n_cls:,}")
            numbers.add(paper.macro_name("main", "class", name[job], cls, "n"), f"{n_cls:,}")
            numbers.add(paper.macro_name("main", "class", name[job], cls, "agree"), f"{agree_cls:,}")
        for arm in paper.ARMS:
            da = d[d.arm == arm]
            n_arm = int(sum(da[f"dec_{k}_n"].fillna(0).sum() for k in ("hold", "up", "down")))
            agree_arm = int(sum(da[f"dec_{k}_agree"].fillna(0).sum() for k in ("hold", "up", "down")))
            print(f"      arm {arm}: {agree_arm:,} of {n_arm:,}")
            numbers.add(paper.macro_name("main", "measured", arm, name[job], "asked"), f"{n_arm:,}")
            numbers.add(paper.macro_name("main", "measured", arm, name[job], "agree"), f"{agree_arm:,}")

    # Seconds over the limit before the first load change, and the planned contrasts with the seconds
    # counted from the load change on (a check of review round 3, not planned).
    phase = seconds_by_phase(args.root, all_runs)
    phase = phase[phase.job.isin(paper.JOB_ORDER)]
    early = phase[phase.slo_before_s >= 5].groupby(["arm", "scenario", "rep"]).ngroups
    early_keys = sorted(set(run_keys(phase[phase.slo_before_s >= 5])))
    print(f"=== seconds over the limit before the load change: 5 s or more for at least one job in {early} of {len(all_runs)} runs "
          f"({sum(k in MARKED for k in early_keys)} marked, {sum(k in day for k in early_keys)} by day)")
    for k in early_keys:
        row = phase[(phase.arm == k[0]) & (phase.scenario == k[1]) & (phase.rep == k[2])]
        print(f"  {k[0]}/{k[1]}/rep{k[2]}: " + ", ".join(f"{name[r.job]} {r.slo_before_s} of {r.slo_before_s + r.slo_after_s}" for r in row.itertuples()))
    numbers.add(paper.macro_name("main", "early runs"), str(early))
    numbers.add(paper.macro_name("main", "disturbed start runs"), str(len(odd)))
    numbers.add(paper.macro_name("main", "disturbed start unmarked"), str(len(set(odd) - set(MARKED))))
    numbers.add(paper.macro_name("main", "noten runs"), str(len(all_runs - ten)))
    after = df.merge(phase, on=["arm", "scenario", "rep", "job"], how="left")
    assert after.slo_after_s.notna().all()
    assert ((after.slo_before_s + after.slo_after_s) == after.slo_violation_s).all(), "the two phases do not add up to the metric"
    after["slo_violation_s"] = after.slo_after_s
    print("=== all runs, seconds counted from the first load change on (not planned)")
    for arm in paper.ARMS:
        for a, b, col, r, adj in tests(after, arm):
            if col != "slo_violation_s":
                continue
            print(f"  {arm:8s} {name[a]:4s} - {name[b]:7s}: {pct(r['mean'])}% [{pct(r['lo'])}, {pct(r['hi'])}] n={r['n']} p={r['p']:.4f}")
            base = ("main", "after", "pooled", arm, name[a], "vs", name[b], "slo")
            for field in ("mean", "lo", "hi"):
                numbers.add(paper.macro_name(*base, field), f"{100 * r[field]:+.0f}".replace("+", "$+$").replace("-", "$-$"))
    numbers.add(paper.macro_name("main", "marked runs"), str(len(MARKED)))
    numbers.add(paper.macro_name("main", "nomark runs"), str(len(all_runs) - len(MARKED)))
    numbers.add(paper.macro_name("main", "day runs"), str(len(day)))
    numbers.add(paper.macro_name("main", "night runs"), str(len(all_runs - day)))

    # Per scenario, in the metric's own unit, with all runs and without the marked ones.
    excluded = []
    for label, keep in blocks[:2]:
        part = df[[k in keep for k in run_keys(df)]]
        print(f"=== {label}: differences per scenario in their own units (seconds, slots)")
        for arm in [a for a in paper.ARMS if (part.arm == a).any()]:
            for a, b in paper.PRIMARY_PAIRS:
                for col in paper.PRIMARY_COLS:
                    cells = []
                    for sc in paper.present(part, part.arm == arm):
                        r = paper.paired(part, arm, sc, a, b, col)
                        if not r:
                            continue
                        out = r["lo"] > 0 or r["hi"] < 0
                        cells.append(f"{sc} {paper.num(r['mean'])} [{paper.num(r['lo'])}, {paper.num(r['hi'])}] "
                                     f"{r['wins']}/{r['n']}{' *' if out else ''}")
                        if out:
                            excluded.append((label, arm, name[a], name[b], SHORT[col], sc, r))
                    print(f"  {arm:8s} {name[a]:4s} - {name[b]:7s} {SHORT[col]:5s}: " + " | ".join(cells))
    print("=== scenario intervals that exclude zero (marked * above; 60 intervals per block, none corrected)")
    for label, arm, a, b, col, sc, r in excluded:
        print(f"  {label}: {arm} {a} - {b} {col} {sc}: {paper.num(r['mean'])} [{paper.num(r['lo'])}, {paper.num(r['hi'])}] "
              f"lower in {r['wins']}/{r['n']}, p={r['p']:.3f}")

    # The wording rules of the amendment of 2026-11-21 13:27 PST, on all runs.
    print("=== wording (all runs)")
    by_name = {v: k for k, v in name.items()}
    pac = by_name["PaC"]
    for model in ("DM", "LLM"):
        job = by_name[model]
        verdicts = []
        for arm in paper.ARMS:
            for col in paper.PRIMARY_COLS:
                r = pooled.get(("all runs", arm, job, pac, col))
                if r:
                    inside = -MARGIN[col] <= r["lo"] and r["hi"] <= MARGIN[col]
                    verdicts.append((arm, col, inside, r))
        every = bool(verdicts) and all(v[2] for v in verdicts)
        widest = max((max(abs(v[3]["lo"]), abs(v[3]["hi"])) for v in verdicts if v[1] == "slo_violation_s"), default=float("nan"))
        print(f"  {model} - PaC: inside the margin in {sum(v[2] for v in verdicts)} of {len(verdicts)} intervals -> "
              + ("not distinguishable from the policy" if every else
                 f"no detectable difference; the interval of the seconds allows up to {100 * widest:.0f}%"))
        above = [f"{v[0]} {SHORT[v[1]]}" for v in verdicts if v[3]["lo"] > 0]
        if above:
            print(f"    pooled interval above zero: {', '.join(above)}")
        under = [f"{v[0]} {SHORT[v[1]]}" for v in verdicts if v[3]["hi"] < 0]
        if under:
            print(f"    pooled interval below zero: {', '.join(under)}")
        numbers.add(paper.macro_name("main", "allows", model), f"{100 * widest:.0f}")
        numbers.add(paper.macro_name("main", "inside", model), str(sum(v[2] for v in verdicts)))
    # What the intervals of the time over the limit allow, over both models and both arms: how much
    # shorter and how much longer a model's job may have been than the policy's.
    ends = [pooled[("all runs", arm, by_name[m], pac, "slo_violation_s")] for arm in paper.ARMS for m in ("DM", "LLM")
            if ("all runs", arm, by_name[m], pac, "slo_violation_s") in pooled]
    if ends:
        shorter, longer = -min(r["lo"] for r in ends), max(r["hi"] for r in ends)
        print(f"  both models, both arms: the intervals allow up to {100 * shorter:.0f}% shorter and {100 * longer:.0f}% longer over the limit")
        numbers.add(paper.macro_name("main", "allows", "shorter"), f"{100 * shorter:.0f}")
        numbers.add(paper.macro_name("main", "allows", "longer"), f"{100 * longer:.0f}")
    spike = paper.paired(df, "accurate", "spike", pac, by_name["Builtin"], "slo_violation_s")
    if spike:
        out = spike["lo"] > 0 or spike["hi"] < 0
        print(f"  scope rule, PaC - Builtin in the spike of the second arm: {paper.num(spike['mean'])} s "
              f"[{paper.num(spike['lo'])}, {paper.num(spike['hi'])}], lower in {spike['wins']}/{spike['n']} -> "
              + ("excludes zero: the sentence on the policy's gain is limited to the sustained overload" if out
                 else "includes zero: the sentence stays as it is"))
    if args.tex:
        numbers.write(args.tex)
        print(f"wrote {args.tex} ({len(set(numbers.lines))} macros)")
        table = os.path.join(os.path.dirname(args.tex), "tab_main_pooled.tex")
        with open(table, "w") as f:
            f.write(pooled_table(table_rows, name))
        print(f"wrote {table}")
        table = os.path.join(os.path.dirname(args.tex), "tab_main_means.tex")
        with open(table, "w") as f:
            f.write(means_table(df, name))
        print(f"wrote {table}")


if __name__ == "__main__":
    main()
