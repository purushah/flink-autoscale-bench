#!/usr/bin/env python3
"""Harness view of the two experiments with extra information for a controller (cluster/plugin):

    python3 expsum.py <campaign dir>            e.g. results-cluster/announced, results-cluster/flapping

One block per run, one line per job: time over the latency limit (whole run, and for a run with
load events: while the first event lasts and after it), the size of Aggregate when the first event
starts, when its first scale-up was decided and when it was in effect (seconds after the event's
start; negative is before it), slots, changes of size, scale-downs undone within five minutes, and
for the jobs with a model how often its choice differed from the policy's.

A job whose last latency summary is more than 30 s before the run's end is marked SHORT, and a job
with latency data for less than 90% of the run's seconds is marked LOW (a stall that slows a job
without stopping it). A run with such a job is left out of the means (ISSUES.md #103, #104).
"""
import calendar
import collections
import csv
import glob
import json
import os
import re
import statistics as st
import sys
import time

LIMIT_MS = 2000
UNDO_SECONDS = 300
MIN_MEASURED = 0.9


def epoch(ts):
    """2026-11-21T02:21:31.388321677Z -> seconds since 1970."""
    whole, _, frac = ts.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


def read_run(d):
    m = json.load(open(os.path.join(d, "meta.json")))
    dur, t0 = m["duration"], m["t0_epoch"]
    events = m.get("events") or []
    e0 = events[0] if events else None
    e1 = events[1] if len(events) > 1 else None

    lat = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(open(os.path.join(d, "latency.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur:
            lat[r["job"]][int(t)].append(float(r["p95_ms"]))

    par = collections.defaultdict(list)  # job -> (t, aggregate, largest vertex), warm-up included
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        if r.get("p_aggregate"):
            sizes = [float(r[k]) for k in ("p_source", "p_aggregate", "p_measure") if r.get(k)]
            par[r["job"]].append((float(r["t"]), float(r["p_aggregate"]), max(sizes)))

    dec = collections.defaultdict(list)
    for line in open(os.path.join(d, "decisions.jsonl")):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        if o.get("event") != "decision":
            continue
        j = re.search(r"name='bench-([^']+)'", o.get("job", ""))
        if j:
            o["t"] = epoch(o["ts"]) - t0
            dec[j.group(1)].append(o)

    rows = []
    for job in m["jobs"]:
        sec = {s: max(v) for s, v in lat.get(job, {}).items()}
        over = [s for s, v in sec.items() if v > LIMIT_MS]
        p = par.get(job, [])
        inrun = [x for x in p if 0 <= x[0] <= dur]
        row = dict(job=job, measured=len(sec), last=max(sec) if sec else -1, short=(max(sec) if sec else -1) < dur - 30,
                   low=len(sec) < MIN_MEASURED * dur,
                   over=len(over), slots=st.mean(x[2] for x in inrun) if inrun else float("nan"))
        if e0 is not None:
            end = e1 if e1 is not None else dur
            row["over_event"] = sum(e0 <= s < end for s in over)
            row["over_after"] = sum(s >= end for s in over)
            before = [x for x in p if x[0] <= e0]
            row["agg_at_event"] = before[-1][1] if before else float("nan")
        # Changes of Aggregate inside the run, with the time the new size was first seen.
        changes = [(b[0], a[1], b[1]) for a, b in zip(p, p[1:]) if a[1] != b[1] and 0 <= b[0] <= dur]
        row["ups"] = sum(c[2] > c[1] for c in changes)
        row["downs"] = sum(c[2] < c[1] for c in changes)
        row["undone"] = sum(
            1 for i, c in enumerate(changes) if c[2] < c[1]
            and any(n[2] > n[1] and n[0] - c[0] <= UNDO_SECONDS for n in changes[i + 1:]))
        if e0 is not None:
            # The first scale-up around the first event: decided (the plugin's log) and in effect (samples).
            first_up = next((c[0] for c in [(b[0], a[1], b[1]) for a, b in zip(p, p[1:]) if b[1] > a[1]] if c[0] >= e0 - 300), None)
            row["up_effect"] = first_up - e0 if first_up is not None else None
        s = job.replace("+", "-")
        answered = [o for o in dec.get(s, []) if o.get("status") == "DECIDED"]
        row["status"] = dict(collections.Counter(o.get("status") for o in dec.get(s, [])))
        if e0 is not None:
            up = next((o["t"] for o in answered if o["t"] >= e0 - 300 and any(
                v.get("name") == "Aggregate" and v.get("target", 0) > v.get("current", 0) for v in o.get("vertices", {}).values())), None)
            row["up_decided"] = up - e0 if up is not None else None
        above = below = same = planned = peak = 0
        for o in answered:
            for v in o.get("vertices", {}).values():
                if v.get("planned_rate") is not None:
                    planned += 1
                if v.get("peak_rate") is not None:
                    peak += 1
                if v.get("rule_choice") is None or not o.get("model"):
                    continue
                if v["target"] > v["rule_choice"]:
                    above += 1
                elif v["target"] < v["rule_choice"]:
                    below += 1
                else:
                    same += 1
        row.update(above=above, below=below, same=same, planned=planned, peak=peak)
        calls = [o["latency_ms"] for o in answered if o.get("model") and o.get("latency_ms") is not None]
        row["call_ms"] = st.median(calls) if calls else None
        rows.append(row)
    return m, rows


def fmt(x, spec="{:.0f}"):
    return "-" if x is None or x != x else spec.format(x)


def main():
    for base in sys.argv[1:]:
        per_job = collections.defaultdict(list)
        runs = used = 0
        for meta in sorted(glob.glob(os.path.join(base, "*", "rep*", "meta.json"))):
            m, rows = read_run(os.path.dirname(meta))
            runs += 1
            ev = m.get("events") or []
            print(f"{m['scenario']} rep {m['rep']} scale {m['scale']} | {m['t0_utc']} to {m['end_utc']} | events at {[round(e) for e in ev][:4]}"
                  f"{' ...' if len(ev) > 4 else ''} | planned: {bool(m.get('planned'))} | drained {m.get('drained')}")
            print(f"   {'job':14s} {'over':>5s} {'event':>6s} {'after':>6s} {'agg@ev':>6s} {'up dec':>7s} {'up eff':>7s} {'slots':>6s} {'up/down':>7s} {'undone':>6s}  model against policy")
            for r in rows:
                model = ""
                if r["above"] + r["below"] + r["same"]:
                    model = f"same {r['same']}, above {r['above']}, below {r['below']}"
                    if r["call_ms"] is not None:
                        model += f", median call {r['call_ms']:.0f} ms"
                extra = []
                if r["planned"]:
                    extra.append(f"planned load seen in {r['planned']} vertex-cycles")
                if r["peak"]:
                    extra.append(f"window peak in {r['peak']}")
                bad = {k: v for k, v in r["status"].items() if k not in ("DECIDED", "COOLDOWN")}
                if bad:
                    extra.append(f"other statuses {bad}")
                print(f"   {r['job']:14s} {r['over']:5d} {fmt(r.get('over_event')):>6s} {fmt(r.get('over_after')):>6s} {fmt(r.get('agg_at_event')):>6s} "
                      f"{fmt(r.get('up_decided'), '{:+.0f}'):>7s} {fmt(r.get('up_effect'), '{:+.0f}'):>7s} {r['slots']:6.2f} {r['ups']:>3d}/{r['downs']:<3d} {r['undone']:6d}  "
                      f"{model}{'; ' if model and extra else ''}{'; '.join(extra)}{'  SHORT' if r['short'] else ''}{f"  LOW ({r['measured']} s measured)" if r['low'] else ''}")
            if not any(r["short"] or r["low"] for r in rows):
                used += 1
                for r in rows:
                    per_job[(m["scenario"], r["job"])].append(r)
            print()
        if per_job:
            print(f"{base}: means per scenario, over {used} of {runs} runs (runs with a SHORT or LOW job are left out)")
            for (scen, job), rs in sorted(per_job.items(), key=lambda kv: kv[0][0]):
                line = f"   {scen:10s} n={len(rs)} {job:14s} over {st.mean(r['over'] for r in rs):6.1f} s ({', '.join(str(r['over']) for r in rs)})"
                if "over_event" in rs[0]:
                    line += f" | in event {st.mean(r['over_event'] for r in rs):6.1f} | after {st.mean(r['over_after'] for r in rs):6.1f}"
                line += f" | slots {st.mean(r['slots'] for r in rs):5.2f} | ups {st.mean(r['ups'] for r in rs):.1f} downs {st.mean(r['downs'] for r in rs):.1f} undone {st.mean(r['undone'] for r in rs):.1f}"
                print(line)
            print()


if __name__ == "__main__":
    main()
