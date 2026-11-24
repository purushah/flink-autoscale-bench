"""Test E of the plan entry of 2026-11-23 08:20 PST: the step-aware demand with the first reading of a
rise taken over two seconds (`lag.step-aware`, operator image cluster12, ISSUES.md #141).
   python3 escore.py <campaign dir, e.g. results-cluster/stepfix2> [scenario]
Each run holds four jobs with the broker-side signal and the option. Per job-run it prints the first
scaling order for Aggregate after the rise, the seconds until the subtasks that cover the peak load
run (the measure of Test A), the sizes of Aggregate, the seconds over the limit and the slots held
(monitoring service, <campaign dir>/datadog.json) and the number of step lines. Then the predictions
that these numbers decide: E3 (first order), E5 (seconds to the covering size) and two of the three
clauses against the remedy. E1, E2, E4 and the third clause are about the step lines: stepnow.py."""
import glob, json, os, sys
import numpy as np
import sigsum, stepscore

ASKED = 10      # E3: the first order asks for at least this many subtasks of Aggregate
WITHIN = 75     # E5: that many subtasks run within this many seconds after the rise
TOO_LATE = 100  # against the remedy: a job-run that takes longer than this


def main():
    camp = sys.argv[1]
    scenario = sys.argv[2] if len(sys.argv) > 2 else "surge"
    runs = sorted(glob.glob(os.path.join(camp, scenario, "rep*")), key=lambda d: int(d.rsplit("rep", 1)[1]))
    dd = json.load(open(os.path.join(camp, "datadog.json")))["runs"] if os.path.exists(os.path.join(camp, "datadog.json")) else {}
    rows = []
    for d in runs:
        if not os.path.exists(os.path.join(d, "meta.json")):
            continue
        m, h, need = sigsum.harness(d)
        rep = int(d.rsplit("rep", 1)[1])
        orders, steps = stepscore.operator(d, m)
        for job in sorted(h):
            x = dd.get(f"{scenario}/{rep}", {}).get(job)
            after = [o for o in orders[job] if o[0] >= 0]
            rows.append(dict(rep=rep, job=job, need=need, first=after[0] if after else None, covers=h[job]["covers"],
                             path=h[job]["path"], over=x[0] if x else float("nan"), slots=(x[2] - x[3]) if x else float("nan"),
                             steps=len([s for s in steps[job] if s[0] >= 0])))
    reps = sorted({r["rep"] for r in rows})
    print(f"{camp}, {scenario}: {len(reps)} runs, {len(rows)} job-runs; Aggregate covers the peak load at {rows[0]['need']} subtasks")
    print("run job: first order after the rise (seconds, sizes), seconds until the covering size runs, sizes of Aggregate, "
          "seconds over the limit, slots held, step lines")
    for r in rows:
        f = r["first"]
        first = f"{f[0]:5.0f} s {f[1]}->{f[2]}" if f else "none"
        print(f"   {r['rep']} {r['job']:24s} {first:14s} {r['covers']:5.0f} s  {r['path']:14s} {r['over']:5.0f} s  {r['slots']:5.2f}  {r['steps']}")
    asked = [r["first"][2] if r["first"] else 0 for r in rows]
    covers = np.array([r["covers"] for r in rows], float)
    print(f"E3  first order asks for at least {ASKED} subtasks: {sum(a >= ASKED for a in asked)} of {len(rows)} job-runs "
          f"(asked: {min(asked)} to {max(asked)})")
    print(f"E5  the covering size runs within {WITHIN} s: {int((covers <= WITHIN).sum())} of {len(rows)} job-runs "
          f"({np.nanmin(covers):.0f} to {np.nanmax(covers):.0f} s; never: {int(np.isnan(covers).sum())})")
    print(f"against the remedy: first order below {ASKED} in {sum(a < ASKED for a in asked)} job-runs (more than one counts); "
          f"job-runs that take more than {TOO_LATE} s: {int((covers > TOO_LATE).sum() + np.isnan(covers).sum())}")
    over, slots = np.array([r["over"] for r in rows], float), np.array([r["slots"] for r in rows], float)
    print(f"seconds over the limit: mean {np.nanmean(over):.0f}, {np.nanmin(over):.0f} to {np.nanmax(over):.0f}; "
          f"slots held: mean {np.nanmean(slots):.2f}")
    for band, pick in (("shipped band", lambda j: "max70" not in j), ("band at the target", lambda j: "max70" in j)):
        o = [r["over"] for r in rows if pick(r["job"])]
        c = [r["covers"] for r in rows if pick(r["job"])]
        print(f"   {band}: {len(o)} job-runs, over the limit mean {np.nanmean(o):.0f} s, covering size after {np.nanmin(c):.0f} to {np.nanmax(c):.0f} s")


if __name__ == "__main__":
    main()
