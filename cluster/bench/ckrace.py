"""For every rescale restart in the saved JobManager logs: the time between the last checkpoint event
before the cancel (trigger or completion) and the cancel, and whether a TaskManager then left by itself.
   python3 ckrace.py <jmlogs dir>"""
import glob, os, re, sys
ansi = re.compile(rb"\x1b\[[0-9;]*m")
def sec(ts):  # 2026-11-21T08:30:51.024
    h, m, s = ts[11:13], ts[14:16], ts[17:23]
    return int(h) * 3600 + int(m) * 60 + float(s)
rows = []
for f in sorted(glob.glob(os.path.join(sys.argv[1], "*.log"))):
    last_trig = last_done = None
    trig_id = done_id = None
    cancel = None
    asked = set(); release_at = None
    for raw in open(f, "rb"):
        line = ansi.sub(b"", raw).decode("utf-8", "replace")
        # use the JobManager's own time stamp (second field), it has milliseconds
        m0 = re.match(r"\S+ (\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d),(\d{3}) ", line)
        if not m0:
            continue
        ts = f"{m0.group(1)}T{m0.group(2)}.{m0.group(3)}"
        m = re.search(r"Triggering checkpoint (\d+)", line)
        if m: last_trig, trig_id = ts, int(m.group(1))
        m = re.search(r"Completed checkpoint (\d+)", line)
        if m: last_done, done_id = ts, int(m.group(1))
        if "need release" in line: release_at = ts
        m = re.search(r"Stopping worker (\S+)\.", line)
        if m and release_at and abs(sec(release_at) - sec(ts)) < 1: asked.add(m.group(1))
        if "switched from state RUNNING to CANCELLING" in line:
            cancel = dict(file=os.path.basename(f)[:28], ts=ts,
                          since_done=(sec(ts) - sec(last_done)) if last_done else None,
                          since_trig=(sec(ts) - sec(last_trig)) if last_trig else None,
                          inflight=(trig_id is not None and (done_id is None or trig_id > done_id)), gone=0)
            rows.append(cancel)
        m = re.search(r"ActiveResourceManager.*Closing TaskExecutor connection (\S+) because: The TaskExecutor is shutting down", line)
        if m and m.group(1) not in asked and cancel and 0 <= sec(ts) - sec(cancel["ts"]) < 2:
            cancel["gone"] += 1
bad = [r for r in rows if r["gone"]]
ok = [r for r in rows if not r["gone"]]
print(f"restarts {len(rows)}, with TaskManagers leaving by themselves {len(bad)}")
for r in bad:
    print("  ", r["file"], r["ts"][11:], f"since completion {r['since_done']*1000:.0f} ms, since trigger {r['since_trig']*1000:.0f} ms, checkpoint in flight {r['inflight']}, TaskManagers gone {r['gone']}")
near = sorted((r["since_done"], r["file"], r["ts"][11:], r["inflight"]) for r in ok if r["since_done"] is not None)
print("other restarts, closest to a checkpoint completion (ms):", [round(x[0] * 1000) for x in near[:12]])
print("other restarts with a checkpoint in flight:", sum(r["inflight"] for r in ok), "of", len(ok))
print("other restarts within 100 ms after a completion:", sum(1 for r in ok if r["since_done"] is not None and r["since_done"] < 0.1))
