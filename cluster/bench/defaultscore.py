"""The cluster signal campaign at the operator's default time settings (plan entry of 2026-11-22 09:06 PST):
per run and job what the predictions D1 to D6 need, and their score.
   python3 defaultscore.py <campaign dir> [scenario]          default scenario: surge_default
Per job-run, from the harness files: the orders for Aggregate after the rise (seconds after it and the
size asked for, from the size the autoscaler asked for in samples.csv), the seconds after the rise at
which Aggregate first runs with the largest size of the run (16 at scale 2), the size it runs with 45
minutes after the rise, the share of the true backlog (end offsets minus positions) that pendingRecords
shows over the first 10 minutes after the rise, the slots in use over the overload, and whether the job
is still over the latency limit in the last 10 s of the measurement (its time is then a lower bound).
From <campaign dir>/datadog.json: seconds over the limit and slots held (monitoring service), which are
the numbers the predictions are scored on.
Works on any campaign with the four jobs of queue_signal.sh (try it on results-cluster/signal surge_long)."""
import collections, csv, json, os, sys
import numpy as np
from scipy import stats

LIMIT = 2000.0
JOBS = ("builtin", "builtin+lagfix", "builtin+max70", "builtin+max70+lagfix")
PAIRS = (("shipped band", "builtin", "builtin+lagfix"), ("band at the target", "builtin+max70", "builtin+max70+lagfix"))


def one(d):
    m = json.load(open(os.path.join(d, "meta.json")))
    dur, rise, fall = m["duration"], m["events"][0], m["events"][1]
    rows = collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur and r.get("p_aggregate"):
            rows[r["job"]].append((t, int(float(r["p_aggregate"])),
                                   int(float(r["target_p_aggregate"])) if r.get("target_p_aggregate") else None,
                                   float(r["end_offset"]) - float(r["position_offset"]) if r.get("end_offset") and r.get("position_offset") else None,
                                   float(r["pending_records"]) if r.get("pending_records") else None,
                                   max(float(r[k]) for k in ("p_source", "p_aggregate", "p_measure") if r.get(k))))
    late = collections.defaultdict(dict)
    for r in csv.DictReader(open(os.path.join(d, "latency.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur:
            s = late[r["job"]]
            s[int(t)] = max(s.get(int(t), 0.0), float(r["p95_ms"]))
    out = {}
    for job in m["jobs"]:
        v = rows[job]
        top = max(p for _, p, _, _, _, _ in v) if v else 0
        orders, asked = [], next((p for t, p, _, _, _, _ in v if t <= rise), v[0][1] if v else 0)
        for t, p, g, _, _, _ in v:
            if g is not None and g != asked and t >= rise:
                orders.append((t - rise, g))
                asked = g
            elif g is not None and t < rise:
                asked = g
        ups = [o for o in orders if True]
        full = next((t - rise for t, p, _, _, _, _ in v if t >= rise and p >= 16), float("nan"))
        at45 = next((p for t, p, _, _, _, _ in v if t >= rise + 2700), float("nan"))
        early = [(true, pend) for t, _, _, true, pend, _ in v if rise <= t <= rise + 600 and true is not None and pend is not None and true >= 5000]
        share = 100 * sum(p for _, p in early) / sum(t for t, _ in early) if early else float("nan")
        slots = [s for t, _, _, _, _, s in v if rise <= t <= fall]
        secs = late[job]
        over = sum(1 for s in secs.values() if s > LIMIT)
        last = [secs.get(s, float("inf")) for s in range(int(dur) - 10, int(dur))]
        out[job] = dict(orders=orders, full=full, at45=at45, share=share, slots=float(np.mean(slots)) if slots else float("nan"),
                        over=over, censored=all(x > LIMIT for x in last), top=top)
    return m, out


def interval(x):
    x = np.asarray(x, float)
    if len(x) < 2:
        return float(x.mean()), float("nan"), float("nan")
    half = float(stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / np.sqrt(len(x)))
    return float(x.mean()), float(x.mean() - half), float(x.mean() + half)


def main():
    camp = sys.argv[1]
    scenario = sys.argv[2] if len(sys.argv) > 2 else "surge_default"
    dd = json.load(open(os.path.join(camp, "datadog.json"))).get("runs", {}) if os.path.exists(os.path.join(camp, "datadog.json")) else {}
    reps = sorted(int(x[3:]) for x in os.listdir(os.path.join(camp, scenario)) if x.startswith("rep") and x[3:].isdigit()
                  and os.path.exists(os.path.join(camp, scenario, x, "meta.json")))
    runs = {}
    for rep in reps:
        m, out = one(os.path.join(camp, scenario, f"rep{rep}"))
        runs[rep] = out
        print(f"run {rep}: rise at {m['events'][0]:.0f} s, fall at {m['events'][1]:.0f} s, measured {m['duration']} s, valid marks: drained {m.get('drained')}, "
              f"settled {m.get('settled')}, taskmanager_fatal {m.get('taskmanager_fatal')}")
        for job in JOBS:
            if job not in out:
                continue
            o, x = out[job], dd.get(f"{scenario}/{rep}", {}).get(job)
            service = f"service {x[0]:.0f} s, {x[2] - x[3]:.1f} slots held" if x else "service: not stored"
            print(f"   {job:22s} over the limit: {service}; harness {o['over']} s{' (still over at the end)' if o['censored'] else ''}; "
                  f"slots in the overload {o['slots']:.1f}; orders " + ", ".join(f"{t:.0f} s -> {g}" for t, g in o["orders"][:8])
                  + f"; 16 subtasks after {o['full']:.0f} s; size after 45 min {o['at45']}; pendingRecords shows {o['share']:.0f}% in the first 10 min")
    if not reps:
        return
    get = lambda rep, job, i: dd.get(f"{scenario}/{rep}", {}).get(job, [float("nan")] * 4)[i]
    print("\nscore (monitoring service for seconds and slots)")
    for band, a, b in PAIRS:
        have = [r for r in reps if not np.isnan(get(r, a, 0) + get(r, b, 0))]
        if not have:
            print(f"   {band}: no monitoring-service numbers stored yet")
            continue
        diff = [get(r, b, 0) - get(r, a, 0) for r in have]
        base = np.mean([get(r, a, 0) for r in have])
        mean, lo, hi = interval(diff)
        slots = [(get(r, b, 2) - get(r, b, 3)) - (get(r, a, 2) - get(r, a, 3)) for r in have]
        print(f"   {band}: broker-side minus default {mean:+.0f} s [{lo:+.0f}, {hi:+.0f}], {100 * mean / base:+.0f}% of the default's {base:.0f} s; by run "
              + ", ".join(f"{x:+.0f}" for x in diff) + f"; lower in {sum(x < 0 for x in diff)} of {len(diff)}; slots {np.mean(slots):+.2f} (by run "
              + ", ".join(f"{x:+.1f}" for x in slots) + ")")
        if band == "shipped band":
            print(f"      D1 (lower in every run, by at least 600 s in every run, at least 20% on average): "
                  f"{'held' if all(x <= -600 for x in diff) and -mean / base >= 0.20 else 'failed'}"
                  f" (smallest gain {-max(diff):.0f} s, mean {-100 * mean / base:.0f}%)")
            print(f"      D5 (more slots with the broker-side signal): {'held' if np.mean(slots) > 0 else 'failed'} in the mean, in {sum(x > 0 for x in slots)} of {len(slots)} runs")
            print(f"      against the finding (longer in two or more runs, or a mean change under 5%): "
                  f"{'yes' if sum(x > 0 for x in diff) >= 2 or -mean / base < 0.05 else 'no'}")
        else:
            print(f"      D6 (lower in all runs but one at most): {'held' if sum(x < 0 for x in diff) >= len(diff) - 1 else 'failed'}")
    b = [runs[r]["builtin+lagfix"] for r in reps if "builtin+lagfix" in runs[r]]
    a = [runs[r]["builtin"] for r in reps if "builtin" in runs[r]]
    if a and b:
        d2 = [bool(o["orders"]) and o["orders"][0][0] <= 180 and o["orders"][0][1] <= 6 and 1200 <= o["full"] <= 1800 for o in b]
        d3 = [bool(o["orders"]) and o["orders"][0][0] > 300 and not o["at45"] >= 16 for o in a]
        d4 = [o["share"] < 15 for o in a]
        print(f"   D2 (broker-side job, shipped band: first order within 180 s for at most 6 subtasks, 16 subtasks 1,200 to 1,800 s after the rise): held in {sum(d2)} of {len(d2)} runs")
        print(f"   D3 (default-signal job, shipped band: first order after more than 300 s, fewer than 16 subtasks 45 min after the rise): held in {sum(d3)} of {len(d3)} runs")
        print(f"   D4 (default-signal job, shipped band: pendingRecords under 15% of the true backlog in the first 10 min): held in {sum(d4)} of {len(d4)} runs")


if __name__ == "__main__":
    main()
