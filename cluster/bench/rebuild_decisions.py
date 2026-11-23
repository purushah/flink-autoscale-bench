"""Rebuild a run's decisions.jsonl from the operator log that the harness followed during the run
(<run>/logs/operator.follow.log), when the log fetched at the end of the run starts late because the
kubelet rotated it (ISSUES.md #109). The saved file is kept as decisions.jsonl.partial.

   python3 rebuild_decisions.py <run dir>...          report only
   python3 rebuild_decisions.py --write <run dir>...  rebuild where the followed log has more records"""
import calendar, json, os, sys, time


def epoch(ts):
    whole, _, frac = ts.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


def records(path, lo, hi):
    """Decision-log records between lo and hi, in order, without the repeats of a restarted follower."""
    seen, out = set(), []
    for raw in open(path, "rb"):
        line = raw.decode("utf-8", "replace")
        i = line.find("JEV_DECISION ")
        if i < 0:
            continue
        payload = line[i + len("JEV_DECISION "):].strip()
        if payload in seen:
            continue
        seen.add(payload)
        try:
            t = epoch(json.loads(payload)["ts"])
        except (ValueError, KeyError):
            continue
        if lo <= t <= hi:
            out.append((t, payload))
    out.sort(key=lambda x: x[0])
    return [p for _, p in out]


write = "--write" in sys.argv
for d in [a for a in sys.argv[1:] if a != "--write"]:
    follow = os.path.join(d, "logs", "operator.follow.log")
    saved = os.path.join(d, "decisions.jsonl")
    if not os.path.exists(follow):
        print(f"{d}: no followed operator log"); continue
    m = json.load(open(os.path.join(d, "meta.json")))
    lo = epoch(m["started_utc"]) if "started_utc" in m else m["t0_epoch"] - 600
    new = records(follow, lo, m["t0_epoch"] + m["duration"] + 120)
    old = [l for l in open(saved).read().splitlines() if l.strip()] if os.path.exists(saved) else []
    missing = len(set(new) - set(old))
    print(f"{d}: saved {len(old)} records, followed log {len(new)}, not in the saved file {missing}")
    if write and missing and len(new) >= len(old) and not os.path.exists(saved + ".partial"):
        os.rename(saved, saved + ".partial")
        with open(saved, "w") as f:
            f.write("\n".join(new) + "\n")
        print("   rebuilt; the saved file is kept as decisions.jsonl.partial")
