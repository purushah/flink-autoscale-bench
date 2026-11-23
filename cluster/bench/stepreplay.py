"""What the step-aware demand (cluster/plugin, lag.step-aware, ISSUES.md #123) would have reported in recorded
cluster runs of the broker-side signal. For each job with the broker-side signal and each evaluation between
the load change and the job's first restart: the demand the plugin reported (window average, from the decision
log), the same recomputed from the harness's end-offset samples, the rate of the newest interval, the growth of
the backlog, and the size of Aggregate that the built-in formula gives with either demand
(demand / target + backlog / catch-up + demand * restart / catch-up, per subtask capacity from the log).
   python3 stepreplay.py <run dir>...
The metric window and the catch-up time are those of each run (60 s; 300 s with `--windows long`, from
meta.json). Not a run of the fix: the job's later behaviour is not simulated."""
import bisect, calendar, csv, json, math, os, sys, time

TARGET, RESTART, MARGIN, RECENT_MIN = 0.7, 30.0, 0.2, 10.0


def epoch(s):
    whole, _, frac = s.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


def interp(xs, ys, x):
    i = bisect.bisect_left(xs, x)
    if i <= 0:
        return ys[0]
    if i >= len(xs):
        return ys[-1]
    f = (x - xs[i - 1]) / (xs[i] - xs[i - 1])
    return ys[i - 1] + f * (ys[i] - ys[i - 1])


for d in sys.argv[1:]:
    m = json.load(open(os.path.join(d, "meta.json")))
    t0, ev = m["t0_epoch"], m["events"]
    WINDOW = CATCH_UP = 300.0 if m.get("windows") == "long" else 60.0
    series = {}
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        if r.get("end_offset") and r.get("position_offset"):
            s = series.setdefault(r["job"], ([], [], [], []))
            s[0].append(float(r["t"])); s[1].append(float(r["end_offset"])); s[2].append(float(r["position_offset"]))
            s[3].append(int(float(r["p_aggregate"])) if r.get("p_aggregate") else 0)
    evals = {}
    for line in open(os.path.join(d, "decisions.jsonl")):
        o = json.loads(line)
        if o.get("event") != "decision":
            continue
        dep = o["job"].split("name='")[1].split("'")[0]
        job = next((j for j, name in m["flink_deployments"].items() if name == dep), None)
        if job is None or "lagfix" not in job:
            continue
        src = next((v for v in o["vertices"].values() if v["name"].startswith("Source")), None)
        agg = next((v for v in o["vertices"].values() if v["name"] == "Aggregate"), None)
        if src and agg and src.get("demand") is not None:
            evals.setdefault(job, []).append((epoch(o["ts"]) - t0, src, agg))
    print(f"{d}: load rises at {ev[0]:.0f} s; metric window and catch-up time {WINDOW:.0f} s")
    for job, es in sorted(evals.items()):
        ts_, ends, pos, pagg = series[job]
        times = [e[0] for e in es]
        for i, (t, src, agg) in enumerate(es):
            if not ev[0] - 1 <= t <= ev[0] + 70 or agg["current"] != es[0][2]["current"]:
                continue
            end = interp(ts_, ends, t)
            lag = max(0.0, end - interp(ts_, pos, t))
            old = [u for u in times[:i] if t - u >= WINDOW]
            ref = old[-1] if old else times[0]
            rate = (end - interp(ts_, ends, ref)) / (t - ref) if t - ref >= 20 else float("nan")
            prev = [u for u in times[:i] if t - u >= RECENT_MIN]
            recent = growth = float("nan")
            if prev:
                p = prev[-1]
                recent = (end - interp(ts_, ends, p)) / (t - p)
                growth = (lag - max(0.0, interp(ts_, ends, p) - interp(ts_, pos, p))) / (t - p)
            aware = recent if recent > rate * (1 + MARGIN) and growth > rate * MARGIN else rate
            cap = agg["capacity_per_subtask"]
            ratio = agg["demand"] / src["demand"] if src["demand"] else 1.0
            size = lambda demand: math.ceil((demand * ratio / TARGET + agg["backlog"] / CATCH_UP + demand * ratio * RESTART / CATCH_UP) / cap)
            print(f"   {job:22s} {t - ev[0]:5.0f} s after the rise  demand logged {src['demand']:6.0f}  window {rate:6.0f}  newest interval {recent:6.0f}"
                  f"  backlog growth {growth:6.0f}  step-aware demand {aware:6.0f}  Aggregate by the formula: {size(src['demand'])} -> {size(aware)}")
