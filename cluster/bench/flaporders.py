"""On-off runs: the orders for Aggregate in the decision log of every job with the policy in code, split
into the first high phase and the later ones.
   python3 flaporders.py <run dir>...
Per job: every evaluation whose plan changes Aggregate (seconds after the last rise or fall, current ->
ordered size, demand, projected demand, the earlier level of the look-back when there is one, backlog),
and the counts of scale-ups and scale-downs ordered from the second high phase on. An order is what the
policy decided; the sizes the job ran with are in flapsum.py.
For a job whose log has the earlier level (prior_peak_rate, operator image cluster10 and later) two checks of
the cap (plan, amendment to Test B): B2, in every evaluation whose demand is not above the earlier level
plus 10% the projected demand is not above the higher of that level and the demand; B5, the scale-ups
ordered from the second high phase on at such an evaluation."""
MARGIN = 0.10
import calendar, json, os, sys, time


def epoch(s):
    whole, _, frac = s.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


total = {}
for d in sys.argv[1:]:
    m = json.load(open(os.path.join(d, "meta.json")))
    t0, ev, dur = m["t0_epoch"], m["events"], m["duration"]
    rises, cut = ev[0::2], ev[2]
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
        if job is None or job.split("+")[0] != "rule":
            continue
        agg = next((v for v in o["vertices"].values() if v["name"] == "Aggregate"), None)
        t = epoch(o["ts"]) - t0
        if agg and 0 <= t <= dur:
            rows.setdefault(job, []).append((t, agg, o.get("status"), o.get("reason")))
    print(f"{d}: rises at " + ", ".join(f"{r:.0f}" for r in rises) + f" s; later phases from {cut:.0f} s")
    for job in [j for j in m["jobs"] if j in rows]:
        rs = sorted(rows[job], key=lambda x: x[0])
        ups = downs = 0
        checked = broken = b5 = 0
        print(f"   {job}: {len(rs)} evaluations")
        for t, a, status, reason in rs:
            prior, demand, proj = a.get("prior_peak_rate"), a.get("demand"), a.get("projected_demand")
            within = prior is not None and demand is not None and demand <= prior * (1 + MARGIN)
            if within and proj is not None:
                checked += 1
                broken += proj > max(prior, demand) + 0.5
                b5 += t >= cut and a["target"] > a["current"]
            if a["target"] == a["current"]:
                continue
            last = max((e for e in ev if e <= t), default=0.0)
            high = last in rises
            later = t >= cut
            ups += later and a["target"] > a["current"]
            downs += later and a["target"] < a["current"]
            f = lambda k: "    -" if a.get(k) is None else f"{a[k]:5.0f}"
            print(f"      {t:5.0f} s ({t - last:3.0f} s into the {'high' if high else 'low '} phase, {'later' if later else 'first'})"
                  f"  {a['current']:2d} -> {a['target']:2d}  demand {f('demand')}  projected {f('projected_demand')}"
                  f"  earlier level {f('prior_peak_rate')}  backlog {f('backlog')}  {status} {reason}")
        print(f"      from the second high phase on: {ups} scale-ups and {downs} scale-downs ordered")
        if checked:
            print(f"      cap: {checked} evaluations with a demand within the margin of the earlier level; projection above"
                  f" the higher of level and demand in {broken} (B2); scale-ups ordered at such an evaluation from the"
                  f" second high phase on: {b5} (B5)")
        t_ = total.setdefault(job, [0, 0, 0, 0, 0, 0])
        t_[0] += ups; t_[1] += downs; t_[2] += 1; t_[3] += checked; t_[4] += broken; t_[5] += b5
if len(sys.argv) > 2:
    print("all runs given, orders for Aggregate from the second high phase on")
    for job, (u, dn, n, checked, broken, b5) in total.items():
        cap = f"; cap: {checked} evaluations within the margin, B2 broken in {broken}, B5 scale-ups {b5}" if checked else ""
        print(f"   {job}: {u} scale-ups, {dn} scale-downs in {n} runs{cap}")
