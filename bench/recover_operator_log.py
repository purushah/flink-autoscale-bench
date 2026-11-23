#!/usr/bin/env python3
"""Rebuild the operator log and the decision log of runs whose saved copy is incomplete.

`bench.py` saves the operator's log with `kubectl logs --since-time`, which returns only the
current log file. The kubelet rotates the operator's log at 10 MiB, so a run that was under way
at a rotation lost everything before it (ISSUES.md #69). The node keeps the rotated files for a
while; copies are in testbed/operator-logs/. This script finds the runs whose saved log starts
late, cuts their time range out of those copies and writes operator.log and decisions.jsonl new.
The saved originals are kept as operator.log.partial and decisions.jsonl.partial.

    python3 bench/recover_operator_log.py            # report only
    python3 bench/recover_operator_log.py --write    # rebuild the affected runs
"""
import argparse
import glob
import gzip
import json
import os
import re
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS = os.path.join(ROOT, "testbed", "operator-logs")
CRI = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)\.(\d+)Z (stdout|stderr) ([FP]) (.*)$")
LINE = re.compile(r"^(?:\x1b\[[0-9;]*m)*(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3})")


def epoch(text, fmt):
    return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp()


def node_lines():
    """Every complete line of the node's copies as (time written, text), oldest first. The CRI
    format splits long lines into partial pieces (flag P) that end with a full piece (flag F)."""
    out, seen = [], set()
    # N.log* is the N-th start of the operator's container: a restart opens 1.log (ISSUES.md #83)
    for path in sorted(glob.glob(os.path.join(LOGS, "[0-9]*.log*"))):
        if path.endswith((".tmp", "collector.log")):
            continue
        opener = gzip.open if path.endswith(".gz") else open
        pending = ""
        with opener(path, "rt", errors="replace") as f:
            for raw in f:
                m = CRI.match(raw.rstrip("\n"))
                if not m:
                    continue
                pending += m.group(5)
                if m.group(4) == "P":
                    continue
                key = (m.group(1), m.group(2), pending)
                if key not in seen:  # the current file is copied again and again
                    seen.add(key)
                    out.append((epoch(m.group(1), "%Y-%m-%dT%H:%M:%S") + float("0." + m.group(2)), pending))
                pending = ""
    out.sort(key=lambda x: x[0])
    return out


def saved_start(path):
    """Time of the first dated line of a saved operator.log, or None."""
    try:
        with open(path, errors="replace") as f:
            for raw in f:
                m = LINE.match(raw)
                if m:
                    return epoch(m.group(1), "%Y-%m-%d %H:%M:%S") + int(m.group(2)) / 1000
    except OSError:
        pass
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--late", type=float, default=45.0, help="a saved log that starts this many seconds after the run is incomplete")
    args = ap.parse_args()
    lines = None
    for meta_path in sorted(glob.glob(os.path.join(ROOT, "results*", "**", "meta.json"), recursive=True)):
        d = os.path.dirname(meta_path)
        if os.path.relpath(d, ROOT).split(os.sep)[0] == "results-cluster":
            continue  # runs of the cluster test: another operator wrote their log (ISSUES.md #109)
        saved = os.path.join(d, "operator.log")
        if not os.path.exists(saved) or os.path.exists(saved + ".partial"):
            continue
        meta = json.load(open(meta_path))
        if "started_utc" not in meta:
            continue
        start = epoch(meta["started_utc"], "%Y-%m-%dT%H:%M:%SZ")
        end = meta["t0_epoch"] + meta["duration"] + 120
        first = saved_start(saved)
        if first is not None and first - start <= args.late:
            continue
        if lines is None:
            lines = node_lines()
            print(f"node copies: {len(lines):,} lines, {datetime.fromtimestamp(lines[0][0], timezone.utc):%F %T} to "
                  f"{datetime.fromtimestamp(lines[-1][0], timezone.utc):%F %T} UTC")
        part = [text for t, text in lines if start <= t <= end]
        lost = (first - start) if first is not None else float("nan")
        lost_measured = max(0.0, min(first, meta["t0_epoch"] + meta["duration"]) - meta["t0_epoch"]) if first is not None else float("nan")
        covered = bool(part) and lines[0][0] <= start
        old_decisions = sum(1 for _ in open(os.path.join(d, "decisions.jsonl"))) if os.path.exists(os.path.join(d, "decisions.jsonl")) else 0
        new_decisions = sum(1 for text in part if "JEV_DECISION " in text)
        print(f"{os.path.relpath(d, ROOT)}: saved log starts {lost:.0f} s late ({lost_measured:.0f} s of the measured phase); "
              f"decisions {old_decisions} -> {new_decisions}; node copies cover it: {covered}")
        if args.write and covered and new_decisions >= old_decisions:
            os.rename(saved, saved + ".partial")
            if os.path.exists(os.path.join(d, "decisions.jsonl")):
                os.rename(os.path.join(d, "decisions.jsonl"), os.path.join(d, "decisions.jsonl.partial"))
            with open(saved, "w") as f:
                f.write("\n".join(part) + "\n")
            with open(os.path.join(d, "decisions.jsonl"), "w") as f:
                for text in part:
                    i = text.find("JEV_DECISION ")
                    if i >= 0:
                        f.write(text[i + len("JEV_DECISION "):] + "\n")


if __name__ == "__main__":
    main()
