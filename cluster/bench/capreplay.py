"""What the cap on the trend projection (cluster/plugin, scale-down.window-caps-trend, ISSUES.md #119) would
have decided in recorded on-off runs of the policy with the look-back (`rule+b`, `rule+c`).
   python3 capreplay.py <run dir>...
For every evaluation of such a job in which the policy ordered a scale-up of Aggregate, from the second
high phase on: the demand, the projection the job used, the highest demand of the look-back window's
samples that were at least 90 s old (the level before the rise), and the scale-up threshold
(ceil((projection / target + backlog / catch-up) / capacity per subtask), as in DecisionEngine.trigger)
 - as logged (recomputed from the logged projection, to check the formula against the log),
 - A: with the projection capped at that level plus the margin (the fix as built on 21 Nov),
 - B: with the projection capped at that level itself, or at the demand if that is higher, while the
      demand is within the margin of the level.
Next to them the size the look-back keeps in a scale-down for the same level and capacity
(ceil(level / target / capacity)), and the same with the margin on the level. A job that kept that size
holds at the jump if the threshold is not above it.
Not a run of the fix: the recorded job scaled up at these evaluations, so its later state is not that
of a job that held. Capacity and backlog are the recorded job's."""
import calendar, json, math, os, sys, time

TARGET, CATCH_UP, WINDOW, EXCLUDE, MARGIN = 0.7, 60.0, 600.0, 90.0, 0.10


def epoch(s):
    whole, _, frac = s.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


def ceil(x):
    return max(1, math.ceil(x - 1e-9))


tot = {"ups": 0, "A": 0, "B": 0, "keepA": 0, "keepB": 0, "same": 0}
phases = {"n": 0, "cycles": 0, "A": 0, "B": 0, "Bcycles": 0, "Bnew": 0}
for d in sys.argv[1:]:
    m = json.load(open(os.path.join(d, "meta.json")))
    t0, ev = m["t0_epoch"], m["events"]
    rises = ev[0::2]
    rows = {}
    for line in open(os.path.join(d, "decisions.jsonl"), errors="replace"):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        if o.get("event") != "decision" or "vertices" not in o:
            continue
        dep = o["job"].split("name='")[1].split("'")[0]
        job = next((j for j, name in m["flink_deployments"].items() if name == dep), None)
        if job is None or not job.startswith("rule+"):
            continue
        agg = next((v for v in o["vertices"].values() if v["name"] == "Aggregate"), None)
        if agg and agg.get("demand") is not None:
            rows.setdefault(job, []).append((epoch(o["ts"]) - t0, agg))
    print(f"{d}: rises at " + ", ".join(f"{r:.0f}" for r in rises) + " s")
    for job, rs in sorted(rows.items()):
        rs.sort(key=lambda x: x[0])
        for i, (t, a) in enumerate(rs):
            if not a["target"] > a["current"] or t < rises[1] - 5:
                continue
            prior = max((b["demand"] for u, b in rs[:i] if EXCLUDE <= t - u <= WINDOW), default=float("nan"))
            cap, backlog, demand, proj = a["capacity_per_subtask"], max(0.0, a.get("backlog") or 0.0), a["demand"], a["projected_demand"]
            trig = lambda p: ceil((p / TARGET + backlog / CATCH_UP) / cap)
            level = prior * (1 + MARGIN)
            within = demand <= level
            pa = min(proj, level) if within else proj
            pb = min(proj, max(prior, demand)) if within else proj
            keep, keep_m = ceil(prior / TARGET / cap), ceil(level / TARGET / cap)
            rise = max((r for r in rises if r <= t + 1), default=rises[0])
            tot["ups"] += 1
            tot["same"] += trig(proj) == a.get("rule_trigger")
            tot["A"] += trig(pa) <= a["current"]
            tot["B"] += trig(pb) <= a["current"]
            tot["keepA"] += trig(pa) <= keep
            tot["keepB"] += trig(pb) <= keep_m
            print(f"   {job:7s} {t - rise:4.0f} s after the rise at {rise:4.0f}  {a['current']:2d} -> {a['target']:2d}  demand {demand:5.0f}  projection {proj:5.0f}"
                  f"  level before {prior:5.0f}  capacity {cap:4.0f}  backlog {backlog:6.0f}  threshold logged {a.get('rule_trigger')} recomputed {trig(proj)}"
                  f" | A: projection {pa:5.0f} threshold {trig(pa):2d} | B: projection {pb:5.0f} threshold {trig(pb):2d}"
                  f" | kept for the level {keep}, with the margin {keep_m}{'' if within else ' | demand above the margin: a new high'}")
        for k, rise in enumerate(rises[1:], start=1):
            end = next((e for e in ev if e > rise), rise + 120.0)
            inside = [(i, t, a) for i, (t, a) in enumerate(rs) if rise <= t <= end]
            if not inside:
                continue
            held = inside[0][2]["current"]
            over_a = over_b = new_high = 0
            for i, t, a in inside:
                prior = max((b["demand"] for u, b in rs[:i] if EXCLUDE <= t - u <= WINDOW), default=float("nan"))
                cap, backlog, demand, proj = a["capacity_per_subtask"], max(0.0, a.get("backlog") or 0.0), a["demand"], a["projected_demand"]
                level = prior * (1 + MARGIN)
                within = demand <= level
                pa = min(proj, level) if within else proj
                pb = min(proj, max(prior, demand)) if within else proj
                over_a += ceil((pa / TARGET + backlog / CATCH_UP) / cap) > held
                over_b += ceil((pb / TARGET + backlog / CATCH_UP) / cap) > held
                new_high += (ceil((pb / TARGET + backlog / CATCH_UP) / cap) > held) and not within
            phases["n"] += 1
            phases["cycles"] += len(inside)
            phases["A"] += over_a > 0
            phases["B"] += over_b > 0
            phases["Bcycles"] += over_b
            phases["Bnew"] += new_high
            if over_b:
                print(f"   {job:7s} high phase from {rise:4.0f} s: size {held} at its first evaluation, {len(inside)} evaluations, threshold above it in {over_a} with A, {over_b} with B")
n = tot["ups"]
if phases["n"]:
    print(f"high phases from the second on: {phases['n']}, with {phases['cycles']} evaluations; a job that stayed at the size it had at the"
          f" phase's first evaluation would have met a threshold above that size in {phases['A']} phases with A and {phases['B']} with B"
          f" ({phases['Bcycles']} evaluations with B; of those {phases['Bnew']} with a demand above the margin, a new high)")
if n:
    print(f"scale-ups from the second high phase on: {n}; logged threshold reproduced in {tot['same']}")
    print(f"  the recorded job (at the size it had) would have held: A in {tot['A']} of {n}, B in {tot['B']} of {n}")
    print(f"  a job that kept the size for the level before would have held: A in {tot['keepA']} of {n};"
          f" B with the margin on the kept size in {tot['keepB']} of {n}")
