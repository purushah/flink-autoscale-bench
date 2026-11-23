"""The two rates behind each "Step in the arrival rate" line of runs with `lag.step-aware`, against the
load that was sent (ISSUES.md #141).
   python3 stepnow.py <run dir>...
For every step line in a run's operator log (both copies, see oplog.py) it takes the newest interval's
rate, the reading within the cycle ("records/s now") and the demand, and divides them by the scenario's
load after the rise (the target rate in load.csv). It lists the first line of each job-run, the range
of both rates at the later lines, the later lines with a demand above 1.2 times the load, and what the
demand would be if the reading within the cycle counted at the first line of a job-run only. The last
part is a recomputation from logged lines, not a run."""
import calendar, csv, json, os, re, sys, time
import oplog

ANSI = re.compile(r"\x1b\[[0-9;]*m")
STEP = re.compile(r"Step in the arrival rate: (\d+) records/s over the last (\d+) s against (\d+) over the window, "
                  r"backlog growing by (-?\d+) records/s, (\S+) records/s now; the demand is (\d+)")
# From operator image cluster12 on the line also says which step line of a rise it is, the seconds between
# the two readings, and at a first line a third reading with its span (ISSUES.md #141).
EXTRA = re.compile(r"\((first|later) step line of this rise, (\S+) s between the two readings(?:, (\d+) records/s over ([\d.]+) s in a third reading)?\)")


def load_after_rise(d, rise):
    rates = [float(r["target_rate"]) for r in csv.DictReader(open(os.path.join(d, "load.csv"))) if float(r["t"]) > rise + 1]
    return rates[0]


def main():
    rows = []
    for d in sys.argv[1:]:
        m = json.load(open(os.path.join(d, "meta.json")))
        t0, rise = m["t0_epoch"], m["events"][0]
        load = load_after_rise(d, rise)
        names = {v: k for k, v in m["flink_deployments"].items()}
        seen = set()
        for line in oplog.lines(d):
            if "Step in the arrival rate" not in line:
                continue
            line = ANSI.sub("", line)
            ts = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d\d\d)", line)
            dep = re.search(r"/(bench-[a-z0-9-]+)\]", line)
            s = STEP.search(line)
            if not ts or not dep or not s:
                print("not parsed:", line[:160])
                continue
            t = calendar.timegm(time.strptime(ts.group(1), "%Y-%m-%d %H:%M:%S")) + int(ts.group(2)) / 1000 - t0 - rise
            key = (d, dep.group(1), round(t))
            if key in seen:  # the same line in both copies of the log
                continue
            seen.add(key)
            now = None if s.group(5) == "unknown" else float(s.group(5))
            x = EXTRA.search(line)
            rows.append({"run": d, "job": names.get(dep.group(1), dep.group(1)), "t": t, "recent": float(s.group(1)),
                         "now": now, "demand": float(s.group(6)), "load": load,
                         "kind": x.group(1) if x else None, "span": x.group(2) if x else None,
                         "third": float(x.group(3)) if x and x.group(3) else None, "third_span": x.group(4) if x and x.group(4) else None})
    first = {}
    for r in rows:
        first.setdefault((r["run"], r["job"]), r)
    print(f"step lines: {len(rows)} in {len(first)} job-runs")
    print("first line of each job-run: seconds after the rise, newest interval, reading within the cycle, demand (share of the load)")
    for (run, job), r in sorted(first.items()):
        now = "unknown" if r["now"] is None else f"{r['now']:.0f}"
        third = "" if r["third"] is None else f"  third reading {r['third']:.0f} ({r['third'] / r['load']:.2f}) over {r['third_span']} s"
        span = "" if r["span"] is None else f"  [{r['kind']} line, {r['span']} s between the two readings]"
        print(f"   {run} {job:24s} {r['t']:6.1f} s  {r['recent']:6.0f} ({r['recent'] / r['load']:.2f})  {now:>7s}  "
              f"{r['demand']:6.0f} ({r['demand'] / r['load']:.2f}){third}{span}")
    low = sum(r["recent"] < 0.9 * r["load"] for r in first.values())
    print(f"   newest interval alone below 0.9 of the load: {low} of {len(first)}")
    later = [r for r in rows if r is not first[(r["run"], r["job"])]]
    print(f"later lines: {len(later)}")
    if later:
        share = lambda k: [r[k] / r["load"] for r in later if r[k] is not None]
        print(f"   newest interval, share of the load: {min(share('recent')):.2f} to {max(share('recent')):.2f}")
        print(f"   reading within the cycle, share of the load: {min(share('now')):.2f} to {max(share('now')):.2f}; "
              f"outside 0.8 to 1.2: {sum(x < 0.8 or x > 1.2 for x in share('now'))}")
        for r in later:
            if r["demand"] > 1.2 * r["load"]:
                print(f"   demand above 1.2 of the load: {r['run']} {r['job']} {r['t']:.0f} s  {r['demand']:.0f} against {r['load']:.0f}")
        print("   with the reading within the cycle at the first line only (recomputed): demand above 1.2 of the load in "
              f"{sum(r['recent'] > 1.2 * r['load'] for r in later)}, below 0.9 in {sum(r['recent'] < 0.9 * r['load'] for r in later)}")
        share = [r["demand"] / r["load"] for r in later]
        print(f"   demand as logged at the later lines, share of the load: {min(share):.2f} to {max(share):.2f}")
    thirds = [r for r in first.values() if r["third"] is not None]
    if thirds:
        d = [r["demand"] / r["load"] for r in thirds]
        n = [r["now"] / r["load"] for r in first.values() if r["now"] is not None]
        print(f"first lines with a third reading: {len(thirds)} of {len(first)}; demand there {min(d):.2f} to {max(d):.2f} of the load, "
              f"outside 0.8 to 1.25: {sum(x < 0.8 or x > 1.25 for x in d)}, above 1.4: {sum(x > 1.4 for x in d)}")
        print(f"half-second reading at the first lines: {min(n):.2f} to {max(n):.2f} of the load, outside 0.8 to 1.25 at "
              f"{sum(x < 0.8 or x > 1.25 for x in n)} of {len(n)}")


if __name__ == "__main__":
    main()
