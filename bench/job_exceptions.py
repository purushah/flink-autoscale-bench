#!/usr/bin/env python3
"""List every job exception that the operator reported and the run it fell into.

The operator writes one audit line per exception it finds in a job's exception history
("Event[Job] | Warning | JOBEXCEPTION | ..."). The node's copies of its log
(testbed/operator-logs/, see recover_operator_log.py) cover the whole life of the testbed, where the
Kubernetes events saved with a run cover only its last hour. A task failure restarts the job's tasks
without changing its state as the sampler sees it, so this is the only complete record of them.

It also lists the starts of the operator's own container, since a restart inside a run changes
what the autoscaler knows (ISSUES.md #82).

Usage: python3 bench/job_exceptions.py            # operator starts, exceptions and affected runs
"""
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from recover_operator_log import ROOT, node_lines  # noqa: E402

AUDIT = re.compile(r"\[flink/(bench-[\w-]+)\] >>> Event\[Job\]\s*\|\s*Warning\s*\|\s*JOBEXCEPTION\s*\|\s*(.*)")


def runs():
    """(start, measured start, end, events, directory) of every saved run; times as epoch seconds."""
    out = []
    for meta_path in glob.glob(os.path.join(ROOT, "results*", "**", "meta.json"), recursive=True):
        # Cluster runs have their own operator. The laptop operator's log says nothing about them, and
        # a laptop event at the same clock time was listed under a cluster run (ISSUES.md #128).
        if os.path.relpath(meta_path, ROOT).split(os.sep)[0] == "results-cluster":
            continue
        try:
            meta = json.load(open(meta_path))
        except (OSError, ValueError):
            continue
        t0 = meta.get("t0_epoch")
        if not t0:
            continue
        d = os.path.dirname(meta_path)
        length = meta.get("duration_s") or meta.get("measure_s")
        if not length:
            try:
                with open(os.path.join(d, "samples.csv")) as f:
                    length = max(float(line.split(",")[0]) for line in list(f)[1:])
            except (OSError, ValueError):
                length = 0
        out.append((began(meta, t0), t0, t0 + length, meta.get("events") or [], os.path.relpath(d, ROOT)))
    return out


def began(meta, t0):
    """When the harness began the run (its own stamp); 10 minutes before the measured phase if it left none.

    A fixed 10 minutes reached back past the run's start: two starts of the operator before a chain was
    started were listed as inside its first run (ISSUES.md #95).
    """
    try:
        stamp = datetime.strptime(meta["started_utc"], "%Y-%m-%dT%H:%M:%SZ")
        return min(stamp.replace(tzinfo=timezone.utc).timestamp(), t0)
    except (KeyError, TypeError, ValueError):
        return t0 - 600


def main():
    all_runs = runs()
    found, starts = [], []
    for t, text in node_lines():
        m = AUDIT.search(text)
        if m:
            found.append((t, m.group(1), m.group(2).split("\tat ")[0].strip()[:110]))
        elif text.strip() == "Starting Operator":
            starts.append(t)
    # The operator's container prints this line once per start; a second one is a restart (ISSUES.md #82).
    print(f"{len(starts)} starts of the operator's container")
    for t in starts:
        hit = [r[4] for r in all_runs if r[0] <= t <= r[2]]
        when = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{when}Z  {', '.join(hit) if hit else 'outside every saved run'}")
    print(f"{len(found)} job exceptions in the operator's log")
    for t, job, what in found:
        hit = [r for r in all_runs if r[0] <= t <= r[2]]
        # Runs never overlap. Where a run left no stamp of its start, the 10 minutes allowed for a
        # warm-up reach back into the run before, so a failure inside one run's measured phase belongs
        # to that run alone.
        hit = [r for r in hit if r[1] <= t] or hit
        when = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if not hit:
            print(f"{when}Z {job:32s} outside every saved run            | {what}")
            continue
        for _, t0, end, events, d in hit:
            phase = "warm-up" if t < t0 else "measured"
            rel = ""
            if events and t >= t0:
                # The disturbance lasts from the first event to the last change of the arrival rate (a
                # ramp has one event, its start, and ends when the rate stops rising; ISSUES.md #134).
                # A step in the keys or in the cost per record has no end inside the run.
                first, last = events[0], max(events[-1], last_rate_change(d))
                at = t - t0
                if first <= at <= last:
                    rel = f", inside the load change: {at - first:.0f} s after it began, {last - at:.0f} s before it ended"
                elif at > last and len(events) == 1 and last == events[0]:
                    rel = f", {at - last:.0f} s after the step in the load, which lasts to the end of the run"
                elif at > last:
                    rel = f", {at - last:.0f} s after the load change ended"
                else:
                    rel = f", {first - at:.0f} s before the load change"
            print(f"{when}Z {job:32s} {d} ({phase}{rel}) | {what}")


def last_rate_change(run_dir):
    """Seconds into the measured phase at which the target arrival rate changes for the last time."""
    import csv
    last, prev = 0.0, None
    try:
        with open(os.path.join(ROOT, run_dir, "load.csv") if not os.path.isabs(run_dir) else os.path.join(run_dir, "load.csv")) as f:
            for r in csv.DictReader(f):
                if not r.get("target_rate"):
                    continue
                t, rate = float(r["t"]), float(r["target_rate"])
                if prev is not None and rate != prev and t >= 0:
                    last = t
                prev = rate
    except OSError:
        pass
    return last


if __name__ == "__main__":
    main()
