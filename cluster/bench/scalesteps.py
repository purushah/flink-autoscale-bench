"""Scale-ups the built-in rule ordered, from the operator's log of each run: seconds after the first load
change, vertex sizes, and the target data rate the rule used.
   python3 scalesteps.py <run dir>...
Reads the operator's log (SCALINGREPORT lines; both copies of a run, see oplog.py) and meta.json. Prints
one line per report and per job the seconds from the load change to the first report."""
import calendar, json, os, re, sys, time
import oplog

ANSI = re.compile(r"\x1b\[[0-9;]*m")
REP = re.compile(r"Parallelism (\d+) -> (\d+) \| Processing capacity ([\d.]+) -> ([\d.]+) \| Target data rate ([\d.]+)")
for d in sys.argv[1:]:
    m = json.load(open(os.path.join(d, "meta.json")))
    t0, ev = m["t0_epoch"], m["events"]
    names = {v: k for k, v in m["flink_deployments"].items()}
    first = {}
    print(f"{d}: load changes at {ev[0]:.0f} s and {ev[1]:.0f} s")
    for line in oplog.lines(d):
        if "SCALINGREPORT" not in line:
            continue
        ts = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)", line)
        dep = re.search(r"/(bench-[a-z0-9-]+)\]", line)
        if not ts or not dep:
            continue
        t = calendar.timegm(time.strptime(ts.group(1), "%Y-%m-%d %H:%M:%S")) - t0
        if not 0 <= t <= m["duration"]:
            continue
        job = names.get(dep.group(1), dep.group(1))
        steps = [f"{a}->{b} (target rate {float(r):.0f})" for a, b, _, _, r in REP.findall(line)]
        first.setdefault(job, t - ev[0])
        print(f"   {t - ev[0]:7.0f} s after the change  {job:22s} {'; '.join(steps)}")
    print("   first report after the change:", {j: round(v) for j, v in sorted(first.items())})
