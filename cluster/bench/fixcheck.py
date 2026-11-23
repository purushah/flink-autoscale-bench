"""Check runs for the TaskManager stop of ISSUES.md #105 from the logs the harness saved in <run>/logs/.
Per run: restarts and TaskManagers that left without a release request (JobManager logs), and in the
TaskManager logs the forced producer closes, the send errors of a closed producer, the close error of
the writer and the fatal exits.
   python3 fixcheck.py <run dir>..."""
import glob, json, os, re, sys

ansi = re.compile(rb"\x1b\[[0-9;]*m")
TM_PATTERNS = [
    ("forced producer closes", "Proceeding to force close the producer"),
    ("'Producer is closed forcefully'", "Producer is closed forcefully"),
    ("writer close errors", "One or more Kafka Producer send requests have encountered exception"),
    ("'exception in exception handler'", "FATAL - exception in exception handler"),
    ("fatal exits", "Fatal error occurred while executing the TaskManager"),
]
# the normal end of a TaskManager that the resource manager has released: the line after the fatal one
RELEASED = re.compile(r"registration at the ResourceManager .* has been rejected")


def lines(path):
    seen = set()  # the saver can fetch a line twice
    for raw in open(path, "rb"):
        if raw in seen:
            continue
        seen.add(raw)
        yield ansi.sub(b"", raw).decode("utf-8", "replace")


def jobmanager(path):
    asked, release_at, cancels, gone, restarts = {}, None, [], [], 0
    for line in lines(path):
        ts = line[:23]
        if "need release" in line:
            release_at = ts
        m = re.search(r"Stopping worker (\S+)\.", line)
        if m and release_at and release_at[:19] == ts[:19]:
            asked[m.group(1)] = ts
        if "switched from state RUNNING to CANCELLING" in line:
            cancels.append(ts)
            restarts += 1
        m = re.search(r"ActiveResourceManager.*Closing TaskExecutor connection (\S+) because: (.*)", line)
        if m and m.group(1) not in asked:
            near = [c for c in cancels if c[:17] == ts[:17]]
            gone.append((ts[11:23], m.group(1), m.group(2).strip()[:50], "cancel at " + near[-1][11:23] if near else "no cancel near"))
    return restarts, gone


total = {"runs": 0, "restarts": 0, "gone": 0, "tm_logs": 0}
total.update({name: 0 for name, _ in TM_PATTERNS})
total["of them after a release"] = 0
for d in sys.argv[1:]:
    meta = json.load(open(os.path.join(d, "meta.json")))
    logs = sorted(glob.glob(os.path.join(d, "logs", "*.log")))
    tm = [f for f in logs if "-taskmanager-" in os.path.basename(f)]
    jm = [f for f in logs if f not in tm and not os.path.basename(f).startswith("operator")]
    restarts, gone = 0, []
    per_job = {}
    for f in jm:
        r, g = jobmanager(f)
        job = re.sub(r"-[0-9a-f]{8,10}-[a-z0-9]{5}\..*$", "", os.path.basename(f))
        per_job[job] = per_job.get(job, 0) + r
        restarts += r
        gone += [(job,) + x for x in g]
    counts = {name: 0 for name, _ in TM_PATTERNS}
    released = 0
    for f in tm:
        fatal = False
        for line in lines(f):
            if fatal and RELEASED.search(line):
                released += 1
            fatal = False
            for name, pat in TM_PATTERNS:
                if pat in line:
                    counts[name] += 1
                    fatal = name == "fatal exits"
    counts["of them after a release"] = released
    print(f"{d}: started {meta.get('started_utc')}, job image {str(meta.get('job_image', '')).rsplit(':', 1)[-1]}, "
          f"taskmanager_fatal {meta.get('taskmanager_fatal')}, "
          f"JobManager logs {len(jm)}, TaskManager logs {len(tm)}")
    print(f"   restarts {restarts} {per_job}; TaskManagers gone without a release request {len(gone)}")
    for g in gone:
        print("      ", *g)
    print("   " + "; ".join(f"{name} {n}" for name, n in counts.items()))
    total["runs"] += 1
    total["restarts"] += restarts
    total["gone"] += len(gone)
    total["tm_logs"] += len(tm)
    for name, n in counts.items():
        total[name] += n
print("all:", json.dumps(total))
