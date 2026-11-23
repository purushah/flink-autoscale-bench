#!/usr/bin/env python3
"""Turns decision logs of benchmark runs into input for the plugin's replay tool
(dev.jevscaler.Replay): one line per autoscaler cycle with what the autoscaler believed about
every vertex of the job.

  python3 bench/replay_states.py out.jsonl results-pilot-v2 results-3way ...
"""
import glob
import json
import os
import sys

SOURCE, AGG = "source", "aggregate"


def role(name):
    return "source" if name.startswith("Source") else "sink" if name.startswith("Measure") else "operator"


def states(root):
    for path in sorted(glob.glob(os.path.join(root, "**", "decisions.jsonl"), recursive=True)):
        run = os.path.relpath(os.path.dirname(path), root)
        for n, line in enumerate(open(path)):
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            vs = d.get("vertices") or {}
            if d.get("event") != "decision" or len(vs) != 3 or any(v.get("demand") is None for v in vs.values()):
                continue
            ids = {role(v["name"]): i for i, v in vs.items()}
            if set(ids) != {"source", "operator", "sink"}:
                continue
            out = []
            for i, v in vs.items():
                r = role(v["name"])
                projected = v.get("projected_demand") or v["demand"]
                out.append({
                    "id": i, "name": v["name"], "role": r,
                    "inputs": [] if r == "source" else [ids["source"]] if r == "operator" else [ids["operator"]],
                    "parallelism": v["current"], "capacity": v["capacity_per_subtask"], "demand": v["demand"],
                    "backlog": v.get("backlog") or 0, "backlog_growth": v.get("backlog_growth") or 0,
                    # The engine projects demand 120 s ahead from the change per minute.
                    "demand_trend": max(0.0, projected / v["demand"] - 1) / 2 if v["demand"] > 0 else 0.0,
                    "align_to": 8 if r == "source" else 120 if r == "operator" else 0,
                    "partitions": 8 if r == "source" else 0, "max_parallelism": 120,
                })
            if all(v["capacity"] and v["capacity"] > 0 for v in out):
                yield {"id": f"{root}/{run}#{n}", "vertices": out, "excluded": [ids["source"]]}


if __name__ == "__main__":
    with open(sys.argv[1], "w") as f:
        n = 0
        for root in sys.argv[2:]:
            for s in states(root):
                f.write(json.dumps(s) + "\n")
                n += 1
    print(n, "states")
