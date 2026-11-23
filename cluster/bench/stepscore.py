"""Test A of the plan entry of 2026-11-21 16:53 PST: the step-aware demand (`lag.step-aware`) on the cluster.
   python3 stepscore.py <campaign dir, e.g. results-cluster/stepfix> [scenario]
Each run holds four jobs with the broker-side signal: `builtin+lagfix` and `builtin+max70+lagfix` (plain) and
the same two with the suffix `+b`, which have the option. Reads each run's operator log (both copies, see oplog.py; first scaling
order after the rise, "Step in the arrival rate" lines), the harness files (sizes of Aggregate) and
<campaign dir>/datadog.json (seconds over the limit, slots held), and scores the predictions A1 to A5.
With the scenario `surge_long` it also scores C1 to C7 of Test C (plan entry of 2026-11-21 21:31 PST):
   python3 stepscore.py results-cluster/stepfix surge_long"""
import calendar, glob, json, math, os, re, sys, time
import numpy as np
import oplog, sigsum

AGGREGATE = "f7c20420360e0172e54933e6abdd481a"
PAIRS = [("shipped band", "builtin+lagfix", "builtin+lagfix+b"), ("band at the target", "builtin+max70+lagfix", "builtin+max70+lagfix+b")]
ANSI = re.compile(r"\x1b\[[0-9;]*m")
ORDER = re.compile(r"Vertex ID " + AGGREGATE + r" \| Parallelism (\d+) -> (\d+) \| Processing capacity [\d.]+ -> [\d.]+ \| Target data rate ([\d.]+)")
STEP = re.compile(r"Step in the arrival rate: ([\d.,E]+) records/s over the last ([\d.,E]+) s against ([\d.,E]+) over the window, "
                  r"backlog growing by ([\d.,E-]+) records/s, (\S+) records/s now; the demand is ([\d.,E]+)")


def operator(d, m):
    """Per job: the scaling orders for Aggregate [(seconds after the rise, from, to, target rate)] and the
    step lines [(seconds after the rise, newest-interval rate, window rate, demand)]."""
    t0, rise = m["t0_epoch"], m["events"][0]
    names = {v: k for k, v in m["flink_deployments"].items()}
    orders, steps = {j: [] for j in m["jobs"]}, {j: [] for j in m["jobs"]}
    for line in oplog.lines(d):
        if "SCALINGREPORT" not in line and "Step in the arrival rate" not in line:
            continue
        ts = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d\d\d)", line)
        dep = re.search(r"/(bench-[a-z0-9-]+)\]", line)
        if not ts or not dep or names.get(dep.group(1)) not in orders:
            continue
        t = calendar.timegm(time.strptime(ts.group(1), "%Y-%m-%d %H:%M:%S")) + int(ts.group(2)) / 1000 - t0
        if not 0 <= t <= m["duration"]:
            continue
        job = names[dep.group(1)]
        o = ORDER.search(line)
        if o and "SCALINGREPORT" in line:
            orders[job].append((t - rise, int(o.group(1)), int(o.group(2)), float(o.group(3))))
        s = STEP.search(line)
        if s:
            num = lambda x: float(x.replace(",", ""))
            steps[job].append((t - rise, num(s.group(1)), num(s.group(3)), num(s.group(6))))
    return orders, steps


def main():
    camp = sys.argv[1]
    scenario = sys.argv[2] if len(sys.argv) > 2 else "surge"
    runs = sorted(glob.glob(os.path.join(camp, scenario, "rep*")), key=lambda d: int(d.rsplit("rep", 1)[1]))
    dd = {}
    if os.path.exists(os.path.join(camp, "datadog.json")):
        dd = json.load(open(os.path.join(camp, "datadog.json"))).get("runs", {})
    rows, unknown = {}, 0
    for d in runs:
        if not os.path.exists(os.path.join(d, "meta.json")):
            continue
        m, h, need = sigsum.harness(d)
        rep = int(d.rsplit("rep", 1)[1])
        orders, steps = operator(d, m)
        for job, v in h.items():
            x = dd.get(f"{scenario}/{rep}", {}).get(job)
            after = [o for o in orders[job] if o[0] >= 0]
            v.update(rep=rep, job=job, dd_over=x[0] if x else float("nan"), dd_slots=(x[2] - x[3]) if x else float("nan"),
                     first=after[0] if after else None, orders=orders[job], steps=steps[job], top=max(int(s) for s in v["path"].split("->")),
                     length=m["events"][1] - m["events"][0])
            rows[(rep, job)] = v
        unknown += sum(1 for line in oplog.lines(d) if "Step in the arrival rate" in line) - sum(len(s) for s in steps.values())
    reps = sorted({k[0] for k in rows})
    print(f"{camp}, {scenario}: {len(reps)} runs; Aggregate covers the peak load at {need} subtasks")
    print("per run and job: first order for Aggregate after the rise (seconds, sizes, target rate), sizes of Aggregate, scale-ups, "
          "seconds until the load is covered, seconds over the limit (monitoring service / harness), slots held, step lines (seconds after the rise: demand)")
    for (rep, job), r in sorted(rows.items()):
        f = r["first"]
        first = f"{f[0]:5.1f} s {f[1]}->{f[2]} ({f[3]:.0f}/s)" if f else "none"
        print(f"   rep {rep:<2} {job:<24} first {first:<24} {r['path']:<14} ups {r['ups']}  covers {r['covers']:>4.0f}  over {r['dd_over']:>4.0f} / {r['over']:>4}  "
              f"slots {r['dd_slots']:>5.2f}  steps " + (", ".join(f"{s[0]:.0f}: {s[3]:.0f}" for s in r["steps"]) or "none"))
    if unknown:
        print(f"   {unknown} step lines could not be read or fall outside a run's window")
    opt = [rows[(k, b)] for k in reps for _, _, b in PAIRS if (k, b) in rows]
    print("\nA1  first order of a job with the option asks for at least 8 subtasks:",
          f"{sum(1 for r in opt if r['first'] and r['first'][2] >= 8)} of {len(opt)}",
          "  below 8:", [(r["rep"], r["job"], round(r["first"][0], 1), r["first"][2]) for r in opt if r["first"] and r["first"][2] < 8],
          "  no order:", [(r["rep"], r["job"]) for r in opt if not r["first"]])
    plain = [rows[(k, a)] for k in reps for _, a, _ in PAIRS if (k, a) in rows]
    print("    for comparison, jobs without the option:", f"{sum(1 for r in plain if r['first'] and r['first'][2] >= 8)} of {len(plain)}")
    print("A2  jobs with the option that reach 16 subtasks with one scale-up:", f"{sum(1 for r in opt if r['ups'] == 1 and r['top'] >= 16)} of {len(opt)}",
          "  (without the option:", f"{sum(1 for r in plain if r['ups'] == 1 and r['top'] >= 16)} of {len(plain)})")
    small, late = [], []
    for band, a, b in PAIRS:
        for k in reps:
            if (k, a) not in rows or (k, b) not in rows:
                continue
            ra, rb = rows[(k, a)], rows[(k, b)]
            gap = rb["covers"] - ra["covers"]
            if not gap <= 10:
                late.append((k, band, ra["covers"], rb["covers"]))
            if ra["first"] and ra["first"][2] < 8:
                small.append((k, band, ra, rb))
    print(f"A3  the job with the option has {need} subtasks no more than 10 s later than its pair: fails in {len(late)} pairs", late)
    print(f"    pairs in which the job without the option ordered fewer than 8 first: {len(small)}")
    for k, band, ra, rb in small:
        print(f"       rep {k} {band:<18} without {ra['first'][0]:.1f} s {ra['first'][1]}->{ra['first'][2]}, covers {ra['covers']:.0f} s, over {ra['dd_over']:.0f} s | "
              f"with {rb['first'][0] if rb['first'] else float('nan'):.1f} s ->{rb['first'][2] if rb['first'] else '?'}, covers {rb['covers']:.0f} s, over {rb['dd_over']:.0f} s | "
              f"sooner by {ra['covers'] - rb['covers']:.0f} s (at least 60: {ra['covers'] - rb['covers'] >= 60}), less over the limit by {ra['dd_over'] - rb['dd_over']:.0f} s (at least 100: {ra['dd_over'] - rb['dd_over'] >= 100})")
    print("A4  seconds over the limit, with the option minus without, paired within runs (monitoring service)")
    for band, a, b in PAIRS:
        diff = [rows[(k, b)]["dd_over"] - rows[(k, a)]["dd_over"] for k in reps if (k, a) in rows and (k, b) in rows and not math.isnan(rows[(k, a)]["dd_over"] + rows[(k, b)]["dd_over"])]
        if len(diff) > 1:
            mean, lo, hi = sigsum.interval(diff)
            print(f"       {band:<18} {mean:+7.1f} [{lo:+7.1f}, {hi:+7.1f}]  lower in {sum(x < 0 for x in diff)} of {len(diff)}   (not longer on average: {mean <= 0})")
    outside = [(r["rep"], r["job"], round(s[0])) for r in rows.values() for s in r["steps"] if not 0 <= s[0] <= 90]
    onplain = [(r["rep"], r["job"], round(s[0])) for r in plain for s in r["steps"]]
    print(f"A5  step lines outside the 90 s after the rise: {len(outside)}", outside[:12], f"  on jobs without the option: {len(onplain)}", onplain[:6])
    for band, a, b in PAIRS:
        diff = [rows[(k, b)]["dd_slots"] - rows[(k, a)]["dd_slots"] for k in reps if (k, a) in rows and (k, b) in rows and not math.isnan(rows[(k, a)]["dd_slots"] + rows[(k, b)]["dd_slots"])]
        if len(diff) > 1:
            mean, lo, hi = sigsum.interval(diff)
            print(f"       slots held, {band:<18} {mean:+6.2f} [{lo:+6.2f}, {hi:+6.2f}]   (at most 0.5 more: {mean <= 0.5})")
    for label, key in (("scale-ups of Aggregate", "ups"), ("seconds until the load is covered", "covers")):
        for band, a, b in PAIRS:
            diff = [rows[(k, b)][key] - rows[(k, a)][key] for k in reps if (k, a) in rows and (k, b) in rows and not math.isnan(rows[(k, a)][key] + rows[(k, b)][key])]
            if len(diff) > 1:
                mean, lo, hi = sigsum.interval(diff)
                print(f"    {label:<34} {band:<18} {mean:+7.2f} [{lo:+7.2f}, {hi:+7.2f}]  mean without {np.mean([rows[(k, a)][key] for k in reps if (k, a) in rows]):.2f}, with {np.mean([rows[(k, b)][key] for k in reps if (k, b) in rows]):.2f}")
    if scenario.endswith("_long"):
        test_c(rows, reps, need, opt, plain)


def test_c(rows, reps, need, opt, plain):
    """Predictions C1 to C7 of Test C (five-minute windows)."""
    print(f"\nTest C ({len(reps)} runs, {len(opt)} job-runs with the option)")
    big = [r for r in opt if r["first"] and r["first"][2] >= need]
    print(f"C1  first order of a job with the option asks for at least {need} subtasks: {len(big)} of {len(opt)} (predicted: at least 10 of 12)",
          "  smaller:", [(r["rep"], r["job"], round(r["first"][0], 1), r["first"][2]) for r in opt if r["first"] and r["first"][2] < need],
          f"  without the option: {sum(1 for r in plain if r['first'] and r['first'][2] >= need)} of {len(plain)}")
    fast = [r for r in opt if r["covers"] <= 120]
    print(f"C2  {need} subtasks run within 120 s after the rise: {len(fast)} of {len(opt)} (predicted: at least 10 of 12)",
          "  seconds with the option:", sorted(round(r["covers"]) for r in opt), "  without:", sorted(round(r["covers"]) for r in plain))
    for label, (band, a, b) in zip(("C3, primary", "C4"), PAIRS):
        pairs = [(rows[(k, a)]["dd_over"], rows[(k, b)]["dd_over"]) for k in reps if (k, a) in rows and (k, b) in rows
                 and not math.isnan(rows[(k, a)]["dd_over"] + rows[(k, b)]["dd_over"])]
        if len(pairs) < 2:
            print(f"{label}  {band}: fewer than two pairs with numbers of the monitoring service")
            continue
        diff = [y - x for x, y in pairs]
        mean, lo, hi = sigsum.interval(diff)
        p = f"  exact sign-flip p {sigsum.sign_flip_p(diff):.4f}" if label.startswith("C3") else ""
        print(f"{label}  {band}: seconds over the limit {np.mean([x for x, _ in pairs]):.0f} without, {np.mean([y for _, y in pairs]):.0f} with; "
              f"{mean:+.1f} [{lo:+.1f}, {hi:+.1f}], lower in {sum(x < 0 for x in diff)} of {len(diff)}{p}"
              + ("  (predicted: 6 of 6 and at least 500 s less; against the fix: fewer than 5 of 6, or under 250 s)" if label.startswith("C3") else "  (predicted: at least 5 of 6)"))
    print(f"C5  one scale-up of Aggregate: {sum(1 for r in opt if r['ups'] == 1)} of {len(opt)} with the option (predicted: at least 10 of 12), "
          f"{sum(1 for r in plain if r['ups'] == 1)} of {len(plain)} without")
    late = [(r["rep"], r["job"], round(s[0])) for r in opt for s in r["steps"] if not 0 <= s[0] <= 180]
    onplain = [(r["rep"], r["job"], round(s[0])) for r in plain for s in r["steps"]]
    after = [(r["rep"], r["job"], round(o[0]), f"{o[1]}->{o[2]}") for r in opt for o in r["orders"] if o[0] > 180 and o[2] > o[1]
             and any(0 <= o[0] - s[0] <= 30 for s in r["steps"] if s[0] > 180)]
    print(f"C6  step lines later than 180 s after the rise: {len(late)}", late[:12], f"  on jobs without the option: {len(onplain)}", onplain[:6],
          f"  scale-ups within 30 s after such a line: {len(after)}", after)
    for band, a, b in PAIRS:
        diff = [rows[(k, b)]["dd_slots"] - rows[(k, a)]["dd_slots"] for k in reps if (k, a) in rows and (k, b) in rows and not math.isnan(rows[(k, a)]["dd_slots"] + rows[(k, b)]["dd_slots"])]
        if len(diff) > 1:
            mean, lo, hi = sigsum.interval(diff)
            print(f"C7  slots held, {band}: {mean:+.2f} [{lo:+.2f}, {hi:+.2f}] (predicted: between 1 and 3 more)")


if __name__ == "__main__":
    main()
