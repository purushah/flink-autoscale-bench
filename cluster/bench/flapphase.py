"""Per-phase view of a flapping-load run: seconds over the limit in each on/off phase, Aggregate size at
each phase start, and the list of Aggregate changes.   python3 flapphase.py <run dir>..."""
import collections, csv, json, os, sys

LIMIT = 2000.0

def run(d):
    m = json.load(open(os.path.join(d, "meta.json")))
    dur = m["duration"]
    ev = [0.0] + list(m["events"]) + [float(dur)]
    lat = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(open(os.path.join(d, "latency.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur:
            lat[r["job"]][int(t)].append(float(r["p95_ms"]))
    par = collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        if r.get("p_aggregate"):
            par[r["job"]].append((float(r["t"]), int(float(r["p_aggregate"]))))
    print(f"{m['scenario']} rep {m['rep']}  events {[round(e) for e in m['events']]}")
    names = []
    for i in range(len(ev) - 1):
        names.append(("off" if i % 2 == 0 else "on") + str(i // 2 + (1 if i % 2 else 0)))
    print("   job            " + " ".join(f"{n:>6}" for n in names) + "   total")
    out = {}
    for job in m["jobs"]:
        sec = {s: max(v) for s, v in lat[job].items()}
        over = [s for s, v in sec.items() if v > LIMIT]
        per = [sum(ev[i] <= s < ev[i + 1] for s in over) for i in range(len(ev) - 1)]
        p = par[job]
        at = []
        for i in range(len(ev) - 1):
            b = [x for x in p if x[0] <= ev[i]]
            at.append(b[-1][1] if b else -1)
        print(f"   {job:<14} " + " ".join(f"{x:>6}" for x in per) + f"   {len(over)}")
        print(f"   {'  agg at start':<14} " + " ".join(f"{x:>6}" for x in at))
        ch = [(round(b[0]), a[1], b[1]) for a, b in zip(p, p[1:]) if a[1] != b[1] and 0 <= b[0] <= dur]
        print("      changes: " + ", ".join(f"{t}s {a}->{b}" for t, a, b in ch))
        out[job] = dict(per=per, at=at, changes=ch, over=len(over))
    return m, out

for d in sys.argv[1:]:
    run(d)
    print()
