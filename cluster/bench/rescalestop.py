"""How long the output stops at each change of size: the time a restart with its restore takes.
   python3 rescalestop.py <campaign dir, e.g. results-cluster/state-alone> [scenario] [--without N[,N]]
For every change of size of Aggregate or Measure between two samples in which the job is RUNNING (the
rescales of statesum.py): the longest stretch without output that begins between 10 s before the last
sample at the old size and 20 s after the first sample at the new size. A stretch that began earlier is
not counted: in the test with every pod alone the long stalls begin while the job runs at an unchanged
size (ISSUES.md #178). Where a change of size falls inside such a stall this measure cannot time the
restore, so the report also counts the changes at which the job was not seen running for more than
30 s between its last sample at the old size and its first at the new one. Not a planned measure; it
describes."""
import csv, glob, json, os, statistics, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import statesum  # noqa: E402

BEFORE_S, AFTER_S = 10, 20
AWAY_S = 30


def _sizes(samples_csv, job, duration):
    """(t, (Aggregate, Measure)) of the samples of the measured phase in which the job is RUNNING."""
    sizes = []
    with open(samples_csv, newline="") as f:
        for r in csv.DictReader(f):
            if r["job"] == job and r.get("state") == "RUNNING" and r.get("p_aggregate") and r.get("p_measure") and 0 <= float(r["t"]) <= duration:
                sizes.append((float(r["t"]), (int(float(r["p_aggregate"])), int(float(r["p_measure"])))))
    return sizes


def seconds_between_sizes(samples_csv, job, duration):
    """Per change of size: the seconds from the last sample at the old size to the first at the new one."""
    sizes = _sizes(samples_csv, job, duration)
    return [int(t1 - t0) if float(t1 - t0).is_integer() else round(t1 - t0, 1)
            for (t0, s0), (t1, s1) in zip(sizes, sizes[1:]) if s0 != s1]


def stops_at_size_changes(latency_csv, samples_csv, job, duration):
    """Per change of size in the measured phase, in the order they happened: the seconds of the longest
    stretch without output that begins at the change (0 when there is none)."""
    quiet, stretches, run = statesum._quiet(latency_csv, job, duration) + [False], [], 0
    for i, q in enumerate(quiet):
        if q:
            run += 1
            continue
        if run:
            stretches.append((i - run, run))
        run = 0
    sizes = _sizes(samples_csv, job, duration)
    return [max((n for start, n in stretches if t0 - BEFORE_S <= start <= t1 + AFTER_S), default=0)
            for (t0, s0), (t1, s1) in zip(sizes, sizes[1:]) if s0 != s1]


def quartiles(values):
    """(first quartile, median, third quartile), None for an empty list."""
    if not values:
        return None
    if len(values) == 1:
        return (values[0],) * 3
    q = statistics.quantiles(values, n=4)
    return tuple(int(x) if float(x).is_integer() else round(x, 1) for x in q)


def main():
    argv = sys.argv[1:]
    without = set()
    if "--without" in argv:
        i = argv.index("--without")
        without = {int(x) for x in argv[i + 1].split(",")}
        argv = argv[:i] + argv[i + 2:]
    camp, scenario = argv[0], argv[1] if len(argv) > 1 else "surge"
    per, away = {}, {}
    runs = sorted((d for d in glob.glob(os.path.join(camp, scenario, "rep*")) if d.rsplit("rep", 1)[1].isdigit()),
                  key=lambda d: int(d.rsplit("rep", 1)[1]))
    n_runs = 0
    for d in runs:
        if int(d.rsplit("rep", 1)[1]) in without or not os.path.exists(os.path.join(d, "meta.json")):
            continue
        m = json.load(open(os.path.join(d, "meta.json")))
        n_runs += 1
        for job in m["jobs"]:
            per.setdefault(job, []).extend(stops_at_size_changes(os.path.join(d, "latency.csv"), os.path.join(d, "samples.csv"), job, m["duration"]))
            away.setdefault(job, []).extend(seconds_between_sizes(os.path.join(d, "samples.csv"), job, m["duration"]))
    left = (" (left out by --without: " + ", ".join(map(str, sorted(without))) + ")") if without else ""
    print(f"{camp}, {scenario}: {n_runs} runs{left}; seconds without output at a change of size")
    for job, v in list(per.items()) + [("both jobs", [x for v in per.values() for x in v])]:
        if v:
            print(f"   {job:<22} {len(v):>3} changes   quartiles {quartiles(v)}   mean {sum(v) / len(v):.1f}   longest {max(v)}   without any stop {sum(x == 0 for x in v)}")
    print("seconds from the last sample at the old size to the first sample at the new size")
    for job, v in list(away.items()) + [("both jobs", [x for v in away.values() for x in v])]:
        if v:
            long = sorted(x for x in v if x > AWAY_S)
            print(f"   {job:<22} not running for more than {AWAY_S} s at {len(long)} of {len(v)} changes"
                  + (" (" + ", ".join(f"{x:g}" for x in long) + " s)" if long else "") + f"   longest of the others {max([x for x in v if x <= AWAY_S], default=0):g}")


if __name__ == "__main__":
    main()
