"""Where the step-aware demand (cluster/plugin, lag.step-aware, ISSUES.md #123) would have reported a step
in recorded cluster runs of the broker-side signal: at every evaluation of every job with that signal,
not only at the rise (that is stepreplay.py).
   python3 stepscan.py <run dir>...
The condition is the plugin's: the records of the newest interval (back to the last evaluation that is
at least 10 s old) arrived faster than the window's average by more than 20%, and the backlog grew by
more than 20% of that average per second in the same interval. End offsets and positions are the
harness's samples, interpolated at the times of the job's evaluations in the decision log.
Counted by where the evaluation lies: before the rise, in the first 90 s after it (180 s with
five-minute windows), later in the overload, after the fall. A step outside the time after the rise
would be a false alarm. Not a run of the option: the recorded jobs did not have it, so what they did
after their first order is not what a job with the option would have done."""
import bisect, calendar, collections, json, os, sys, time

MARGIN, RECENT_MIN = 0.2, 10.0


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


import csv

total = collections.Counter()
fired = collections.Counter()
for d in sys.argv[1:]:
    m = json.load(open(os.path.join(d, "meta.json")))
    t0, ev = m["t0_epoch"], m["events"]
    long_windows = m.get("windows") == "long"
    window = 300.0 if long_windows else 60.0
    near = 180.0 if long_windows else 90.0
    series = {}
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        if r.get("end_offset") and r.get("position_offset"):
            s = series.setdefault(r["job"], ([], [], []))
            s[0].append(float(r["t"])); s[1].append(float(r["end_offset"])); s[2].append(float(r["position_offset"]))
    times = {}
    for line in open(os.path.join(d, "decisions.jsonl"), errors="replace"):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        if o.get("event") != "decision":
            continue
        dep = o["job"].split("name='")[1].split("'")[0]
        job = next((j for j, name in m["flink_deployments"].items() if name == dep), None)
        if job is not None and "lagfix" in job and job in series:
            times.setdefault(job, []).append(epoch(o["ts"]) - t0)
    for job, ts in sorted(times.items()):
        ts.sort()
        xs, ends, pos = series[job]
        for i, t in enumerate(ts):
            if t < xs[0] or t > xs[-1]:
                continue
            # As in the plugin: the window's average reaches back to the newest evaluation that is a
            # full window old, or to the oldest one there is while the job is younger than that.
            seen = [u for u in ts[:i] if u >= xs[0]]
            old = [u for u in seen if t - u >= window]
            prev = [u for u in seen if t - u >= RECENT_MIN]
            if not prev or (not old and t - seen[0] < min(20.0, window / 2)):
                continue
            end = interp(xs, ends, t)
            lag = max(0.0, end - interp(xs, pos, t))
            ref, p = (old[-1] if old else seen[0]), prev[-1]
            rate = (end - interp(xs, ends, ref)) / (t - ref)
            recent = (end - interp(xs, ends, p)) / (t - p)
            growth = (lag - max(0.0, interp(xs, ends, p) - interp(xs, pos, p))) / (t - p)
            where = ("before the rise" if t < ev[0] else "first %d s after the rise" % near if t < ev[0] + near
                     else "later in the overload" if t < ev[1] else "after the fall")
            kind = "long" if long_windows else "short"
            total[(kind, where)] += 1
            step = recent > rate * (1 + MARGIN) and growth > rate * MARGIN
            fired[(kind, where)] += step
            # Shown: a step outside the time after the rise, and an evaluation in that time without one.
            if step != where.startswith("first"):
                if True:
                    print(f"   {d} {job:22s} {t:6.0f} s ({where}, rise at {ev[0]:.0f}, fall at {ev[1]:.0f}; {'step' if step else 'no step'}): newest interval {recent:6.0f} over {t - p:4.1f} s,"
                          f" window {rate:6.0f}, backlog growth {growth:6.0f} records/s, backlog {lag:8.0f}")
for kind in ("short", "long"):
    for where in ("before the rise", "first 90 s after the rise", "first 180 s after the rise", "later in the overload", "after the fall"):
        if total[(kind, where)]:
            print(f"{kind} windows, {where}: a step at {fired[(kind, where)]} of {total[(kind, where)]} evaluations")
