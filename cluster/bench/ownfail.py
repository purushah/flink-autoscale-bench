"""Test G's rule for runs that disturbed_cluster.py marks (plan entry of 2026-11-24 02:50 PST). A marked run
still counts if every marked event is a heartbeat timeout of a TaskManager whose pod has no Kubernetes
event of an eviction, a preemption or a lost node: that TaskManager failed under the job's own load.
   python3 ownfail.py <campaign dir> [scenario]
Prints per run: clean, own (counts; the result is also given without these runs) or disturbed (repeated).
It reads what disturbed_cluster.py reads and no outcome.

The third test with state (plan entry of 2026-11-24, runs with --apart) adds two things. A TaskManager
that shut itself down after a checkpoint of its job expired is the job's own failure as well, if no
TaskManager pod of that job has such a Kubernetes event; without --apart it stays a disturbance, because
jobs on shared nodes slow each other's disks. And a run with --apart in which a node held pods of both
jobs does not count (shared). Runs with --alone (every Flink pod on a node of its own) are runs with the
jobs apart; one in which a node held two Flink pods of the harness at the same time does not count
(crowded). "Held" means at the same time and in this run: placements before the run's start are left
out, and a pod is on its node from its placement to the event that stopped it."""
import glob
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import disturbed_cluster  # noqa: E402

CLUSTER = {"Evicted", "Preempted", "Preempting", "NodeNotReady", "TaintManagerEviction", "Disrupted", "DisruptionTerminating",
           "Drain", "Unconsolidatable", "NodeShutdown", "Terminated", "TerminationGracePeriodExpiring"}
TIMEOUT = re.compile(r"heartbeat of TaskManager with id (\S+) timed out")
SHUTDOWN = "The TaskExecutor is shutting down"
EXPIRED = "Checkpoint expired before completing"
ASSIGNED = re.compile(r"Successfully assigned \S+/(\S+) to (\S+)")


def classify(inside, event_items, apart=False):
    """"clean", "own" or "disturbed" from the events that disturbed_cluster.check marked inside the window
    and the items of the run's events.json. apart: the run kept its jobs on nodes of their own."""
    if not inside:
        return "clean"
    name = lambda e: (e.get("involvedObject") or {}).get("name") or ""
    hit = {name(e) for e in event_items if e.get("reason") in CLUSTER}
    for w in inside:
        if not w["what"].startswith("TaskManager"):
            return "disturbed"
        why = w.get("why") or ""
        m = TIMEOUT.search(why)
        if m:
            if m.group(1) in hit:
                return "disturbed"
        elif apart and why.startswith(SHUTDOWN):
            expired = any(e.get("reason") == "JobException" and name(e) == w["job"] and EXPIRED in (e.get("message") or "")
                          for e in event_items)
            if not expired or any(pod.startswith(w["job"] + "-taskmanager-") for pod in hit):
                return "disturbed"
        else:
            return "disturbed"
    return "own"


def kept_apart(meta):
    """Whether the run kept its jobs on nodes of their own (--apart, or --alone, which is stricter)."""
    return bool(meta.get("apart") or meta.get("alone"))


def stays(event_items, pod_items, since=None):
    """[(node, job, from, to)] for every Flink pod of this harness: the time it was on its node, from its
    placement event (or its creation, for a pod of the pod list) to the event that stopped it. None is
    an open end. since: the run's start as an epoch; the cluster keeps events for an hour, so the events
    of a run also hold the placements of the run before it, under the same pod names. Those are left out."""
    stamp = lambda e: disturbed_cluster.epoch(e.get("lastTimestamp") or e.get("eventTime") or e.get("firstTimestamp"))
    placed, stopped = {}, {}
    for e in event_items:
        pod, t = (e.get("involvedObject") or {}).get("name") or "", stamp(e)
        if since is not None and (t is None or t < since):
            continue
        if e.get("reason") == "Scheduled":
            m = ASSIGNED.search(e.get("message") or "")
            if m and job_of(m.group(1)):
                placed[m.group(1)] = (m.group(2), t)
        elif e.get("reason") == "Killing" and job_of(pod) and t is not None:
            stopped[pod] = min(t, stopped.get(pod, t))
    for p in pod_items:
        pod, node = p["metadata"]["name"], (p.get("spec") or {}).get("nodeName")
        if job_of(pod) and node and pod not in placed:
            placed[pod] = (node, disturbed_cluster.epoch(p["metadata"].get("creationTimestamp")))
    return [(node, job_of(pod), t, stopped.get(pod)) for pod, (node, t) in sorted(placed.items())]


def together(event_items, pod_items, since, other_job_only):
    """The nodes on which two Flink pods of this harness were at the same time (of two jobs, if asked)."""
    by_node = {}
    for node, job, a, b in stays(event_items, pod_items, since):
        by_node.setdefault(node, []).append((job, float("-inf") if a is None else a, float("inf") if b is None else b))
    return sorted(node for node, rows in by_node.items()
                  if any(x[1] < y[2] and y[1] < x[2] and (x[0] != y[0] or not other_job_only)
                         for i, x in enumerate(rows) for y in rows[i + 1:]))


def crowded_nodes(pod_items, event_items=(), since=None):
    """The nodes that held more than one Flink pod of this harness at the same time."""
    return together(event_items, pod_items, since, other_job_only=False)


def job_of(pod):
    """The FlinkDeployment a pod of this harness belongs to, None for other pods."""
    if not pod.startswith("bench-"):
        return None
    return pod.split("-taskmanager-")[0] if "-taskmanager-" in pod else pod.rsplit("-", 2)[0]


def shared_nodes(event_items, pod_items, since=None):
    """The nodes that held pods of more than one job at the same time, from the run's placement events
    and its pod list."""
    return together(event_items, pod_items, since, other_job_only=True)


def main():
    camp, scenario = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "*")
    runs = sorted((d for d in glob.glob(os.path.join(camp, scenario, "rep*")) if d.rsplit("rep", 1)[1].isdigit()),
                  key=lambda d: (os.path.dirname(d), int(d.rsplit("rep", 1)[1])))
    for d in runs:
        r = disturbed_cluster.check(d)
        if r is None:
            continue
        items = (disturbed_cluster.load(os.path.join(d, "events.json")) or {}).get("items", [])
        meta = disturbed_cluster.load(os.path.join(d, "meta.json")) or {}
        apart = kept_apart(meta)
        v = classify(r["inside"], items, apart)
        if apart:
            pods = (disturbed_cluster.load(os.path.join(d, "pods.json")) or {}).get("items", [])
            since = disturbed_cluster.epoch(meta.get("started_utc"))
            both = shared_nodes(items, pods, since)
            if both:
                print(f"{r['run']}: shared; {len(both)} node(s) held pods of both jobs at the same time")
                continue
            crowd = crowded_nodes(pods, items, since) if meta.get("alone") else []
            if crowd:
                print(f"{r['run']}: crowded; {len(crowd)} node(s) held more than one Flink pod of the test at the same time")
                continue
        print(f"{r['run']}: {v}" + ("" if v == "clean" else "; " + "; ".join(f"{w['job']} at {w['t_after_first_change_s']} s: {w.get('why') or w['what']}"[:160] for w in r["inside"])))


if __name__ == "__main__":
    main()
