"""Run inside the driver pod on a finished run, before the run is copied out (the pods' logs stay in the
pod). Writes <run dir>/disturb.json with what is needed to tell whether the cluster moved a pod of a job
under the run (plan entry of 2026-11-22 14:30 PST, ISSUES.md #135). It reads no latency and no sizes.

  jobmanagers   per job, every JobManager pod whose log the run followed, with the first and the last time
                stamp of that log. More than one pod means that the JobManager pod was replaced.
  unasked       per job, every TaskManager whose connection the JobManager closed without the resource
                manager having asked for its release in the same second (the logic of tmstops.py), with
                the time and Flink's reason, cut to 80 characters.
  taskmanagers  per job, the number of TaskManager pods the run followed.
  operator      the test operator's pods at the time of the scan, with their creation times.

    python3 podscan.py <run dir> [output file]      default output: <run dir>/disturb.json
"""
import glob
import json
import os
import re
import subprocess
import sys

ANSI = re.compile(rb"\x1b\[[0-9;]*m")
STAMP = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d+)?Z ")
TM = re.compile(r"-taskmanager-\d+-\d+$")


def job_of(pod):
    """bench-builtin-lagfix-taskmanager-1-2 and bench-builtin-lagfix-5546db67cd-csgh2 -> bench-builtin-lagfix"""
    if TM.search(pod):
        return TM.sub("", pod)
    return pod.rsplit("-", 2)[0]


def clean(text):
    text = re.sub(r"\S*(s3|s3a|s3p)://\S*", "<path>", text)
    return text.strip()[:80]


def scan(run_dir):
    jms, tms, unasked = {}, {}, {}
    for path in sorted(glob.glob(os.path.join(run_dir, "logs", "bench-*.log"))):
        pod = os.path.basename(path).rsplit(".", 2)[0]
        job = job_of(pod)
        if TM.search(pod):
            tms[job] = tms.get(job, 0) + 1
            continue
        first = last = None
        asked, release_at, seen = set(), None, set()
        with open(path, "rb") as f:
            for raw in f:
                if raw in seen:  # a follower that was started again repeats lines
                    continue
                seen.add(raw)
                line = ANSI.sub(b"", raw).decode("utf-8", "replace")
                m = STAMP.match(line)
                if not m:
                    continue
                ts = m.group(1)
                first = first or ts
                last = ts
                if "need release" in line:
                    release_at = ts
                m2 = re.search(r"Stopping worker (\S+)\.", line)
                if m2 and release_at == ts:
                    asked.add(m2.group(1))
                m3 = re.search(r"Closing TaskExecutor connection (\S+) because: (.*)", line)
                if m3 and m3.group(1) not in asked:
                    unasked.setdefault(job, []).append({"t": ts + "Z", "taskmanager": m3.group(1), "why": clean(m3.group(2))})
        jms.setdefault(job, []).append({"pod": pod, "first": first and first + "Z", "last": last and last + "Z"})
    return jms, tms, unasked


def operator_pods():
    try:
        ns = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip()
        out = subprocess.run(
            ["kubectl", "-n", ns, "get", "pods", "-l", "app.kubernetes.io/name=asbench-operator", "-o",
             'jsonpath={range .items[*]}{.metadata.name} {.metadata.creationTimestamp} {.status.phase}{"\\n"}{end}'],
            capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return [dict(zip(("pod", "created", "phase"), line.split())) for line in out.splitlines() if line.strip()]


def main():
    run_dir = sys.argv[1]
    jms, tms, unasked = scan(run_dir)
    for pods in jms.values():
        pods.sort(key=lambda p: p["first"] or "")
    out = {"jobmanagers": jms, "taskmanagers": tms, "unasked": unasked, "operator": operator_pods()}
    with open(sys.argv[2] if len(sys.argv) > 2 else os.path.join(run_dir, "disturb.json"), "w") as f:
        json.dump(out, f, indent=1)
    print("podscan:", {j: len(p) for j, p in sorted(jms.items())}, "JobManager pods;",
          sum(len(v) for v in unasked.values()), "TaskManagers gone without a release request;",
          len(out["operator"] or []), "operator pods")


if __name__ == "__main__":
    main()
