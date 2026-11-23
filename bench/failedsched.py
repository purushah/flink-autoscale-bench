"""Pods that could not be placed (FailedScheduling) in the saved Kubernetes events of laptop runs,
between the run's start (started_utc) and its end (the time meta.json was written).
   python3 failedsched.py <campaign dir>...
Per run: each pod with a FailedScheduling event, the time of its first such event and the time it was
placed (Scheduled), in seconds of the run. Uses no outcome."""
import calendar, collections, glob, json, os, sys, time

def stamp(e):
    for key in ("eventTime", "firstTimestamp", "lastTimestamp"):
        ts = e.get(key)
        if ts:
            return calendar.timegm(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S"))
    return None

def last(e):
    ts = e.get("lastTimestamp") or (e.get("series") or {}).get("lastObservedTime") or e.get("eventTime") or e.get("firstTimestamp")
    return calendar.timegm(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")) if ts else None

tot = collections.Counter()
for root in sys.argv[1:]:
    runs = sorted(glob.glob(os.path.join(root, "**", "events.json"), recursive=True))
    hits = 0
    for path in runs:
        d = os.path.dirname(path)
        try:
            ev = json.load(open(path)); meta = json.load(open(os.path.join(d, "meta.json")))
        except (OSError, ValueError):
            continue
        items = ev.get("items", ev) if isinstance(ev, dict) else ev
        t0 = meta.get("t0_epoch") or 0
        try:
            begin = calendar.timegm(time.strptime(meta["started_utc"], "%Y-%m-%dT%H:%M:%SZ"))
        except (KeyError, ValueError):
            begin = t0 - 600
        end = os.path.getmtime(os.path.join(d, "meta.json"))
        failed, placed = {}, {}
        for e in items:
            name = e.get("involvedObject", {}).get("name", "")
            when = stamp(e)
            if when is None or not (begin <= when <= end):   # the saved events reach back an hour, into earlier runs
                continue
            if e.get("reason") == "FailedScheduling":
                a, b = stamp(e), last(e)
                cur = failed.get(name)
                failed[name] = (min(a, cur[0]) if cur else a, max(b, cur[1]) if cur else b, (cur[2] if cur else 0) + (e.get("count") or (e.get("series") or {}).get("count") or 1), (e.get("message") or "")[:60])
            elif e.get("reason") == "Scheduled":
                placed[name] = max(when, placed.get(name, 0))
        for name, (a, b, n, msg) in sorted(failed.items()):
            hits += 1
            p = placed.get(name)
            wait = (p - a) if p else None
            print(f"  {d}: {name} first {a - t0:.0f} s, last {b - t0:.0f} s, events {n}, placed {('%.0f s (waited %.0f s)' % (p - t0, wait)) if p else 'never in the saved events'} | {msg}")
    print(f"{root}: {len(runs)} runs with saved events, {hits} pods with a failed placement")
