"""What the source's pendingRecords shows of the true backlog in the cluster runs. The harness samples,
every five seconds per job, the partitions' end offsets (from the brokers), the readers' positions and
pendingRecords (from the job). Per job-run: over the samples with a true backlog (end offsets minus
positions) of at least MIN records, the sum of pendingRecords divided by the sum of the true backlog, and
the median of the per-sample shares. Also the median of end offsets minus positions before the load
rises, which shows how far apart the two readings are in time. With --by-size the same share is split
by the parallelism of Aggregate at the sample (2, 3 to 6, 7 to 15, 16 or more), per job over all runs,
with the range over the runs that have at least three such samples.
   python3 signalcheck.py [--min 5000] [--by-size] <run dir>..."""
import collections, csv, json, os, statistics as st, sys

args = sys.argv[1:]
MIN = 5000
if args and args[0] == "--min":
    MIN = int(args[1]); args = args[2:]
BY_SIZE = bool(args) and args[0] == "--by-size"
if BY_SIZE:
    args = args[1:]
band_of = lambda p: "2" if p <= 2 else ("3-6" if p <= 6 else ("7-15" if p < 16 else "16+"))
size_tot = collections.defaultdict(lambda: collections.defaultdict(lambda: [0.0, 0.0, 0]))
size_run = collections.defaultdict(list)
rows_out = []
for d in args:
    m = json.load(open(os.path.join(d, "meta.json")))
    first = m["events"][0] if m.get("events") else 0
    per = collections.defaultdict(list)
    size_one = collections.defaultdict(lambda: collections.defaultdict(lambda: [0.0, 0.0, 0]))
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        if not (r.get("end_offset") and r.get("position_offset") and r.get("pending_records")):
            continue
        t = float(r["t"])
        if not 0 <= t <= m["duration"]:
            continue
        per[r["job"]].append((t, float(r["end_offset"]) - float(r["position_offset"]), float(r["pending_records"])))
        if BY_SIZE and r.get("p_aggregate") and per[r["job"]][-1][1] >= MIN:
            b = band_of(int(float(r["p_aggregate"])))
            for acc in (size_tot[r["job"]][b], size_one[r["job"]][b]):
                acc[0] += per[r["job"]][-1][2]; acc[1] += per[r["job"]][-1][1]; acc[2] += 1
    for job, bands in size_one.items():
        for b, (pend, true, n) in bands.items():
            if n >= 3:
                size_run[(job, b)].append(100 * pend / true)
    for job in m["jobs"]:
        s = per.get(job, [])
        quiet = [b for t, b, p in s if t < first - 10]
        over = [(b, p) for t, b, p in s if b >= MIN]
        if not over:
            rows_out.append((d, job, None)); continue
        share = sum(p for b, p in over) / sum(b for b, p in over)
        rows_out.append((d, job, dict(n=len(over), share=share, med=st.median(p / b for b, p in over),
                                      peak=max(b for b, p in over), quiet=st.median(quiet) if quiet else float("nan"))))
shares = []
for d, job, r in rows_out:
    if r is None:
        print(f"{d:<46} {job:<14} no sample with a backlog of {MIN} records"); continue
    shares.append(r["share"])
    print(f"{d:<46} {job:<14} samples {r['n']:>3}  shown {100 * r['share']:5.1f}% (median per sample {100 * r['med']:5.1f}%)  "
          f"largest backlog {r['peak']:>9,.0f}  quiet-phase end minus position {r['quiet']:>6.0f}")
if shares:
    print(f"job-runs with a backlog: {len(shares)}; share shown {100 * min(shares):.1f}% to {100 * max(shares):.1f}%, median {100 * st.median(shares):.1f}%")
if BY_SIZE:
    print("share shown by the parallelism of Aggregate at the sample")
    for job in sorted(size_tot):
        for b in ("2", "3-6", "7-15", "16+"):
            pend, true, n = size_tot[job][b]
            if not n:
                continue
            v = size_run.get((job, b), [])
            runs = f"runs with 3 or more samples: {len(v)}, {min(v):.1f}% to {max(v):.1f}%, median {st.median(v):.1f}%" if v else "no run with 3 samples"
            print(f"   {job:<22} Aggregate at {b:<5} samples {n:>4}  shown {100 * pend / true:5.1f}%   {runs}")
