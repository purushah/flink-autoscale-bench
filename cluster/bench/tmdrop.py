"""TaskManagers lost at a restart (ISSUES.md #105), found in the harness's samples alone. A hit: the
number of TaskManager pods of a job falls between two samples although the sample before and the sample
at the fall both show a parallelism (running or asked for) that needs more pods than are left (4 slots
per pod). A release after a scale-down is not a hit, because there the sample before already asks for
the smaller size. On the cluster runs with saved logs it finds 10 of the 12 events that the logs show;
the two it misses are scale-downs after which the pods that were left still held the new size.
   python3 tmdrop.py <campaign dir>...      (a directory with runs below it; results-v3 and results-e5
                                              are skipped while their campaigns run, unless --all)"""
import collections, csv, glob, math, os, sys

SLOTS = 4
args = [a for a in sys.argv[1:] if a != "--all"]
everything = "--all" in sys.argv


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def want(r):
    v = [num(r.get(k)) for k in ("p_source", "p_aggregate", "p_measure", "target_p_source", "target_p_aggregate", "target_p_measure")]
    v = [x for x in v if x is not None]
    return max(v) if v else None


total = [0, 0, 0, 0]
for camp in args:
    if not everything and os.path.basename(os.path.normpath(camp)) in ("results-v3", "results-e5"):
        print(f"{camp}: skipped (campaign still running)")
        continue
    files = sorted(glob.glob(os.path.join(camp, "**", "samples.csv"), recursive=True))
    hits, jobs, changes = [], 0, 0
    for fn in files:
        rows = collections.defaultdict(list)
        with open(fn) as fh:
            for r in csv.DictReader(fh):
                rows[r["job"]].append(r)
        for job, rs in rows.items():
            jobs += 1
            for a, b, i in ((rs[i - 1], rs[i], i) for i in range(1, len(rs))):
                t = num(b["t"])
                if t is None or t <= 0:
                    continue
                pa = (a["p_source"], a["p_aggregate"], a["p_measure"])
                pb = (b["p_source"], b["p_aggregate"], b["p_measure"])
                if pa != pb and "" not in pa and "" not in pb:
                    changes += 1
                tma, tmb = num(a.get("tm_pods")), num(b.get("tm_pods"))
                if tma is None or tmb is None or tmb >= tma:
                    continue
                wa, wb = want(a), want(b)
                if wa is None or wb is None:
                    continue
                if math.ceil(wa / SLOTS) > tmb and math.ceil(wb / SLOTS) > tmb:
                    ran = [int(num(c["p_aggregate"])) for c in rs[i:i + 9]
                           if c["state"] == "RUNNING" and num(c["p_aggregate"]) is not None and num(c["t"]) - t <= 40]
                    hits.append((os.path.dirname(fn), job, f"{t:.0f} s", b["state"], f"pods {int(tma)} -> {int(tmb)}",
                                 f"asked {int(wb)}", f"ran with {ran[:6]}"))
    print(f"{camp}: {len(files)} runs, {jobs} job-runs, {changes} changes of parallelism in the samples, {len(hits)} hits")
    for h in hits:
        print("   ", *h)
    total = [x + y for x, y in zip(total, (len(files), jobs, changes, len(hits)))]
print(f"all: {total[0]} runs, {total[1]} job-runs, {total[2]} changes of parallelism, {total[3]} hits")
