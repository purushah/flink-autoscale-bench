"""How long the TaskManagers of each job waited before they could work: from a pod's first event (its
placement, or the first failed try to place it) to the start of its last container. With every pod on
a node of its own (--alone) a TaskManager may have to wait for a new node, and the job that asks first
takes the nodes that are free. From the run's events.json; reads no outcome.
   python3 nodewait.py <campaign dir> [scenario]
Prints per run and job: the number of TaskManagers that came during the run, the seconds each waited,
their sum."""
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import disturbed_cluster  # noqa: E402


def waits(event_items, since=None):
    """{job: [seconds, ...]} for the TaskManager pods with an event from `since` (an epoch) on, in the
    order they came. None for a pod whose containers did not start."""
    first, started = {}, {}
    for e in event_items:
        pod = (e.get("involvedObject") or {}).get("name") or ""
        if not pod.startswith("bench-") or "-taskmanager-" not in pod:
            continue
        stamps = [t for t in (disturbed_cluster.epoch(e.get(k)) for k in ("firstTimestamp", "eventTime", "lastTimestamp")) if t]
        if not stamps or (since is not None and min(stamps) < since):
            continue
        if e.get("reason") in ("Scheduled", "FailedScheduling"):
            first[pod] = min(first.get(pod, min(stamps)), min(stamps))
        elif e.get("reason") == "Started":
            started[pod] = max(started.get(pod, max(stamps)), max(stamps))
    out = {}
    for pod in sorted(first, key=lambda p: (first[p], p)):
        out.setdefault(pod.split("-taskmanager-")[0], []).append(round(started[pod] - first[pod]) if pod in started else None)
    return out


def main():
    camp, scenario = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "*")
    runs = sorted((d for d in glob.glob(os.path.join(camp, scenario, "rep*")) if d.rsplit("rep", 1)[1].isdigit()),
                  key=lambda d: (os.path.dirname(d), int(d.rsplit("rep", 1)[1])))
    for d in runs:
        meta = disturbed_cluster.load(os.path.join(d, "meta.json"))
        if meta is None:
            continue
        items = (disturbed_cluster.load(os.path.join(d, "events.json")) or {}).get("items", [])
        w = waits(items, disturbed_cluster.epoch(meta.get("started_utc")))
        print(os.path.join(*d.rstrip("/").split("/")[-2:]) + ": " + "; ".join(
            f"{job}: {len(v)} TaskManagers, waited {', '.join('?' if s is None else str(s) for s in v)} s, "
            f"sum {sum(s for s in v if s is not None)} s" for job, v in sorted(w.items())))


if __name__ == "__main__":
    main()
