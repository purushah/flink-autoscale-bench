"""False-alarm runs (queue_falsealarm.sh): what each job did around a spike that was announced and did
not come. Times are relative to the announced start. Per job: the changes of Aggregate, the slots it
held (largest vertex parallelism) from 150 s before the announced start to the end of the run, the
slots of its TaskManager pods, the seconds over the latency limit and the restarts in the whole run.
   python3 falsesum.py <run dir>..."""
import collections, csv, json, os, statistics as st, sys

LIMIT = 2000.0


def collapse(p):
    """p: [(t, state)]. Changes [(t, a, b)], states that lasted under 12 s dropped (restart readings)."""
    runs = []
    for t, s in p:
        if runs and runs[-1][2] == s:
            runs[-1][1] = t
        else:
            runs.append([t, t, s])
    keep = [r for i, r in enumerate(runs) if i in (0, len(runs) - 1) or (runs[i + 1][0] - r[0]) >= 12]
    merged = []
    for r in keep:
        if merged and merged[-1][2] == r[2]:
            merged[-1][1] = r[1]
        else:
            merged.append(list(r))
    return [(b[0], a[2], b[2]) for a, b in zip(merged, merged[1:])]


def run(d):
    m = json.load(open(os.path.join(d, "meta.json")))
    dur, t0 = m["duration"], m["t0_epoch"]
    start = m["planned"]["event_epoch"] - t0
    end = m["planned"]["event_end_epoch"] - t0
    lat = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(open(os.path.join(d, "latency.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur:
            lat[r["job"]][int(t)].append(float(r["p95_ms"]))
    agg = collections.defaultdict(list); both = collections.defaultdict(list)
    slots = collections.defaultdict(list); pods = collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        t = float(r["t"])
        if r.get("tm_pods"):
            pods[r["job"]].append((t, 4 * int(float(r["tm_pods"]))))
        if r.get("p_aggregate"):
            sizes = [float(r[k]) for k in ("p_source", "p_aggregate", "p_measure") if r.get(k)]
            agg[r["job"]].append((t, int(float(r["p_aggregate"]))))
            slots[r["job"]].append((t, max(sizes)))
            if r.get("p_measure"):
                both[r["job"]].append((t, (int(float(r["p_aggregate"])), int(float(r["p_measure"])))))
    print(f"{m['scenario']} rep {m['rep']}: announced spike from {start:.0f} to {end:.0f} s of the run "
          f"({m['planned']['rate_before']} to {m['planned']['rate_during']} records/s), no event: {m.get('no_event')}")
    out = {}
    for job in m["jobs"]:
        over = sum(1 for s, v in lat[job].items() if max(v) > LIMIT)
        ch = collapse(agg[job])
        win = [x[1] for x in slots[job] if start - 150 <= x[0] <= dur]
        pw = [x[1] for x in pods[job] if start - 150 <= x[0] <= dur]
        restarts = len([c for c in collapse(both[job]) if start - 150 <= c[0] <= dur])
        out[job] = dict(over=over, slots=st.mean(win) if win else float("nan"), pods=st.mean(pw) if pw else float("nan"),
                        restarts=restarts, changes=ch)
        print(f"   {job:<8} over {over:>3} s  slots {out[job]['slots']:5.2f}  pod slots {out[job]['pods']:5.2f}  restarts {restarts}  "
              f"Aggregate: " + (", ".join(f"{t - start:+.0f}s {a}->{b}" for t, a, b in ch) or "no change"))
    return m, out


res = [run(d) for d in sys.argv[1:]]
if len(res) > 1:
    print("means over the runs given")
    for job in res[0][0]["jobs"]:
        f = lambda k: st.mean(o[job][k] for _, o in res)
        print(f"   {job:<8} over {f('over'):5.1f} s  slots {f('slots'):5.2f}  pod slots {f('pods'):5.2f}  restarts {f('restarts'):3.1f}")
