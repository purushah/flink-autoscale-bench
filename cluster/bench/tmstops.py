"""Scan saved JobManager logs for TaskManagers that disconnected by themselves ("The TaskExecutor is
shutting down") without the resource manager having asked for their release just before.
   python3 tmstops.py <jmlogs dir>"""
import glob, os, re, sys
ansi = re.compile(rb"\x1b\[[0-9;]*m")
total = [0, 0, 0, 0]
for f in sorted(glob.glob(os.path.join(sys.argv[1], "*.log"))):
    asked = {}       # tm -> time of "Stopping worker" by a release
    release_at = None
    cancels = []
    rows = []
    restarts = 0
    seen = set()     # the saver can fetch a line twice (it asks for the log since the last round)
    for raw in open(f, "rb"):
        if raw in seen:
            continue
        seen.add(raw)
        line = ansi.sub(b"", raw).decode("utf-8", "replace")
        ts = line[:23]
        if "need release" in line:
            release_at = ts
        m = re.search(r"Stopping worker (\S+)\.", line)
        if m and release_at and release_at[:19] == ts[:19]:
            asked[m.group(1)] = ts
        if "switched from state RUNNING to CANCELLING" in line:
            cancels.append(ts); restarts += 1
        m = re.search(r"ActiveResourceManager.*Closing TaskExecutor connection (\S+) because: (.*)", line)
        if m:
            tm, why = m.group(1), m.group(2).strip()
            if tm in asked:
                continue
            near = [c for c in cancels if c[:17] == ts[:17]]
            rows.append((ts, tm, why[:60], "cancel at " + near[-1][11:23] if near else "no cancel in that 10 s"))
    name = os.path.basename(f)
    print(f"{name}: restarts {restarts}, TaskManagers gone without a release request {len(rows)}")
    for r in rows:
        print("    ", r[0][11:23], r[1], "|", r[2], "|", r[3])
    total = [a + b for a, b in zip(total, (1, restarts, len(rows), len({r[3] for r in rows})))]
print(f"all: logs {total[0]}, restarts {total[1]}, TaskManagers gone without a release request {total[2]}, "
      f"restarts with such a TaskManager {total[3]}")
