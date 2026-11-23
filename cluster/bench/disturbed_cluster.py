"""Marks the runs of a cluster campaign in which the cluster moved a pod of a job under the run (plan
entry of 2026-11-22 14:30 PST, ISSUES.md #135). A run is disturbed if, between the first and the last
change of the load (for surge_default: the rise and the end of the overload), the JobManager pod of a job
was replaced, or the JobManager closed the connection of a TaskManager that the resource manager had not
asked to release. It reads meta.json, disturb.json (made in the driver pod by podscan.py from the pods'
logs), pods.json and events.json. It reads no latency, no backlog and no sizes.

    python3 disturbed_cluster.py <campaign dir> [scenario] [--json out.json]

Also printed, without a verdict: the same things outside that window, a test operator pod created while
the run lasted, and Kubernetes events on the jobs' pods whose reason is not one of the everyday ones
(events live for an hour, so the list covers the end of a long run only).
"""
import calendar
import glob
import json
import os
import sys
import time

EVERYDAY = {"Pulled", "Created", "Started", "Pulling", "Scheduled", "Killing", "Unhealthy", "SuccessfulCreate",
            "ScalingReplicaSet", "JobStatusChanged", "Scaling", "SpecChanged", "Submit", "ScalingReport",
            "Configuration recommendation", "ScalingVetoed", "IneffectiveScaling", "SuccessfulDelete",
            "StatusChanged", "Cleanup", "ScalingExecutor", "Rollback", "Suspended", "RecoverDeployment",
            "RestartUnhealthyJob", "Missing", "ScalingLimited", "MemoryPressure", "FailedScheduling"}


def epoch(stamp):
    return calendar.timegm(time.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S")) if stamp else None


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def check(run_dir):
    meta = load(os.path.join(run_dir, "meta.json"))
    if meta is None:
        return None
    t0, events = meta["t0_epoch"], meta.get("events") or []
    lo = t0 + (events[0] if events else 0)
    hi = t0 + (events[-1] if len(events) > 1 else meta["duration"])
    start, end = epoch(meta.get("started_utc")) or t0 - meta.get("warmup", 0), t0 + meta["duration"]
    rel = lambda t: round(t - lo)
    out = {"run": os.path.join(*run_dir.rstrip("/").split("/")[-2:]), "window_s": round(hi - lo),
           "inside": [], "outside": [], "operator": [], "events": [], "scan": True}
    scan = load(os.path.join(run_dir, "disturb.json"))
    if scan is None:
        out["scan"] = False
        scan = {"jobmanagers": {}, "unasked": {}, "operator": None}
    for job, pods in sorted(scan["jobmanagers"].items()):
        for pod in pods[1:]:  # every JobManager pod after the first one is a replacement
            t = epoch(pod["first"])
            what = {"job": job, "what": "JobManager pod replaced", "t_after_first_change_s": rel(t)}
            (out["inside"] if lo <= t <= hi else out["outside"]).append(what)
    for job, rows in sorted(scan["unasked"].items()):
        for r in rows:
            t = epoch(r["t"])
            what = {"job": job, "what": "TaskManager gone without a release request", "t_after_first_change_s": rel(t),
                    "why": r["why"]}
            (out["inside"] if lo <= t <= hi else out["outside"]).append(what)
    # The pod list at the end of the run says the same about JobManagers without the logs: a JobManager
    # pod that was created well after the run's start is a replacement.
    pods = load(os.path.join(run_dir, "pods.json")) or {"items": []}
    for p in pods.get("items", []):
        name = p["metadata"]["name"]
        if "-taskmanager-" in name:
            continue
        created = epoch(p["metadata"].get("creationTimestamp"))
        if created and created > start + 300:
            job = name.rsplit("-", 2)[0]
            known = any(w["job"] == job and w["what"].startswith("JobManager") for w in out["inside"] + out["outside"])
            if not known:
                what = {"job": job, "what": "JobManager pod replaced (pod list)", "t_after_first_change_s": rel(created)}
                (out["inside"] if lo <= created <= hi else out["outside"]).append(what)
    for p in scan.get("operator") or []:
        created = epoch(p.get("created"))
        if created and start <= created <= end:
            out["operator"].append({"what": "test operator pod created during the run", "t_after_first_change_s": rel(created)})
    ev = load(os.path.join(run_dir, "events.json")) or {"items": []}
    for e in ev.get("items", []):
        if e.get("reason") in EVERYDAY:
            continue
        t = epoch(e.get("lastTimestamp") or e.get("firstTimestamp") or e.get("eventTime"))
        out["events"].append({"reason": e.get("reason"), "object": (e.get("involvedObject") or {}).get("name"),
                              "t_after_first_change_s": rel(t) if t else None})
    out["disturbed"] = bool(out["inside"])
    out["jobs_hit"] = sorted({w["job"] for w in out["inside"]})
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    json_out = sys.argv[sys.argv.index("--json") + 1] if "--json" in sys.argv else None
    if json_out in args:
        args.remove(json_out)
    camp, scenario = args[0], (args[1] if len(args) > 1 else "*")
    runs = sorted(glob.glob(os.path.join(camp, scenario, "rep*")), key=lambda d: (os.path.dirname(d), int(d.rsplit("rep", 1)[1]) if d.rsplit("rep", 1)[1].isdigit() else 0))
    results = [r for r in (check(d) for d in runs if os.path.isdir(d) and ".failed" not in d) if r]
    for r in results:
        verdict = "DISTURBED (" + ", ".join(r["jobs_hit"]) + ")" if r["disturbed"] else ("clean" if r["scan"] else "clean by the pod list; no disturb.json")
        print(f"{r['run']}: {verdict}; window {r['window_s']} s")
        for key in ("inside", "outside", "operator", "events"):
            for w in r[key]:
                print(f"    {key}: " + ", ".join(f"{k} {v}" for k, v in w.items()))
    n = sum(r["disturbed"] for r in results)
    print(f"{camp}: {len(results)} runs, {n} disturbed, {sum(not r['scan'] for r in results)} without disturb.json")
    if json_out:
        with open(json_out, "w") as f:
            json.dump(results, f, indent=1)


if __name__ == "__main__":
    main()
