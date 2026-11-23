"""The backlog signal in closed loop on the cluster (plan entry of 2026-11-21 13:10 PST): broker-side signal
against default signal within each band, paired within runs.
   python3 sigsum.py <campaign dir, e.g. results-cluster/signal> [scenario] [--one-test] [--no-test] [--without N[,N]] [--tex <file> <macro prefix>]
With --tex it also writes the numbers as LaTeX macros (monitoring service for seconds and slots, harness
files for the counts). With --one-test the plan names one primary test, the shipped band, without a
correction (the long-window campaign, entry of 2026-11-21 14:06 PST): the macro for its p-value is the
exact one and the band at the target gets none. With --no-test no p-value is computed or written (the
plan's rule for a campaign that ends with fewer runs than planned: pairs, t interval and signs). With
--without the runs with these numbers are left out (runs that the cluster disturbed, marked by
disturbed_cluster.py before any outcome was read).
Primary numbers are those of the monitoring service (<campaign dir>/datadog.json: per run and job
[seconds over the limit, seconds measured, slots registered, slots free]). The harness files give the
same quantities, the scale-ups of Aggregate, the restarts and the time until Aggregate runs with enough
subtasks for the peak load at nominal capacity (500 records/s per subtask)."""
import collections, csv, glob, json, math, os, sys
import numpy as np
from scipy import stats

LIMIT, NOMINAL = 2000.0, 500.0
PAIRS = [("shipped band", "builtin", "builtin+lagfix"), ("band at the target", "builtin+max70", "builtin+max70+lagfix")]


def collapse(p):
    """[(t, size)] samples -> changes [(t, from, to)], states of under 12 s dropped (as flapsum.py)."""
    runs = []
    for t, s in p:
        if runs and runs[-1][2] == s:
            runs[-1][1] = t
        else:
            runs.append([t, t, s])
    keep = [r for i, r in enumerate(runs) if i in (0, len(runs) - 1) or (runs[i + 1][0] - r[0]) >= 12]
    merged = []
    for r in keep:
        if merged and merged[-1][2] == r[2]:
            merged[-1][1] = r[1]
        else:
            merged.append(list(r))
    return [(b[0], a[2], b[2]) for a, b in zip(merged, merged[1:])]


def harness(d):
    m = json.load(open(os.path.join(d, "meta.json")))
    dur, ev = m["duration"], m["events"]
    peak = max(float(r["target_rate"]) for r in csv.DictReader(open(os.path.join(d, "load.csv"))) if r.get("target_rate"))
    need = math.ceil(peak / NOMINAL)
    lat = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(open(os.path.join(d, "latency.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur:
            lat[r["job"]][int(t)].append(float(r["p95_ms"]))
    agg, both, slots = collections.defaultdict(list), collections.defaultdict(list), collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        t = float(r["t"])
        if not 0 <= t <= dur or not r.get("p_aggregate"):
            continue
        sizes = [float(r[k]) for k in ("p_source", "p_aggregate", "p_measure") if r.get(k)]
        agg[r["job"]].append((t, int(float(r["p_aggregate"]))))
        slots[r["job"]].append(max(sizes))
        if r.get("p_measure"):
            both[r["job"]].append((t, (int(float(r["p_aggregate"])), int(float(r["p_measure"])))))
    out = {}
    for job in m["jobs"]:
        over = sum(1 for s, v in lat[job].items() if max(v) > LIMIT)
        ch = collapse(agg[job])
        covers = next((t - ev[0] for t, p in agg[job] if t >= ev[0] and p >= need), float("nan"))
        out[job] = dict(over=over, slots=float(np.mean(slots[job])), ups=sum(1 for c in ch if c[2] > c[1]),
                        restarts=len(collapse(both[job])), covers=covers,
                        path="->".join([str(ch[0][1])] + [str(c[2]) for c in ch]) if ch else str(agg[job][0][1]))
    return m, out, need


def sign_flip_p(diff):
    """Exact two-sided sign-flip p-value for the mean of paired differences (all sign patterns)."""
    diff = np.asarray(diff, float)
    n, observed = len(diff), abs(diff.mean()) - 1e-12
    count = sum(1 for k in range(1 << n) if abs(sum(d if (k >> i) & 1 else -d for i, d in enumerate(diff)) / n) >= observed)
    return count / (1 << n)


def interval(x):
    x = np.asarray(x, float)
    half = float(stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / math.sqrt(len(x))) if len(x) > 1 else float("nan")
    return float(x.mean()), float(x.mean() - half), float(x.mean() + half)


def formula_sizes(runs, cap=16):
    """What the rule's formula asked for against the maximum, per job-run with the broker-side signal:
    (first size above the maximum while Aggregate still ran below it, seconds after the rise at that
    evaluation, largest size after the rise). The plugin logs the formula's size at every evaluation
    (rule_size in decisions.jsonl: Equation 1 with the restart term, before the maximum is applied)."""
    import calendar, time
    out = []
    for d in runs:
        path = os.path.join(d, "decisions.jsonl")
        if not os.path.exists(path) or not os.path.exists(os.path.join(d, "meta.json")):
            continue
        meta = json.load(open(os.path.join(d, "meta.json")))
        rise = meta["t0_epoch"] + meta["events"][0]
        per_job = collections.defaultdict(list)
        for line in open(path):
            if not line.strip():
                continue
            r = json.loads(line)
            job = r["job"].split("name='")[1].split("'")[0] if "name='" in r.get("job", "") else r.get("job", "")
            if "lagfix" not in job:
                continue
            ts = r["ts"]
            t = calendar.timegm(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")) + float("0." + (ts[20:].rstrip("Z") or "0")) - rise
            for v in (r.get("vertices") or {}).values():
                if v.get("name") == "Aggregate" and v.get("rule_size") is not None and t >= 0:
                    per_job[job].append((t, v["current"], v["rule_size"]))
        for job, rows in sorted(per_job.items()):
            first = next(((size, t) for t, cur, size in rows if cur < cap and size > cap), None)
            if first:
                out.append((os.path.basename(d), job, first[0], first[1], max(size for _, _, size in rows)))
    return out


def main():
    argv = sys.argv[1:]
    one_test = "--one-test" in argv
    no_test = "--no-test" in argv
    argv = [x for x in argv if x not in ("--one-test", "--no-test")]
    without = set()
    if "--without" in argv:
        i = argv.index("--without")
        without = {int(x) for x in argv[i + 1].split(",")}
        argv = argv[:i] + argv[i + 2:]
    tex = None
    if "--tex" in argv:
        i = argv.index("--tex")
        tex = (argv[i + 1], argv[i + 2])
        argv = argv[:i] + argv[i + 3:]
    camp = argv[0]
    scenario = argv[1] if len(argv) > 1 else "surge"
    runs = sorted((d for d in glob.glob(os.path.join(camp, scenario, "rep*")) if d.rsplit("rep", 1)[1].isdigit()),
                  key=lambda d: int(d.rsplit("rep", 1)[1]))
    runs = [d for d in runs if int(d.rsplit("rep", 1)[1]) not in without]
    dd = {}
    if os.path.exists(os.path.join(camp, "datadog.json")):
        dd = json.load(open(os.path.join(camp, "datadog.json"))).get("runs", {})
    rows = []
    for d in runs:
        if not os.path.exists(os.path.join(d, "meta.json")):
            continue
        m, h, need = harness(d)
        rep = int(d.rsplit("rep", 1)[1])
        for job, v in h.items():
            x = dd.get(f"{scenario}/{rep}", {}).get(job)
            v.update(rep=rep, job=job, dd_over=x[0] if x else float("nan"), dd_measured=x[1] if x else float("nan"),
                     dd_slots=(x[2] - x[3]) if x else float("nan"))
            rows.append(v)
    reps = sorted({r["rep"] for r in rows})
    print(f"{camp}, {scenario}: {len(reps)} runs{' (left out: ' + ', '.join(map(str, sorted(without))) + ')' if without else ''}; "
          f"Aggregate covers the peak load at {need} subtasks")
    print("per run: seconds over the limit (monitoring service / harness), slots held (service) and in use (harness), scale-ups of Aggregate, restarts, seconds until the load is covered, sizes of Aggregate")
    for r in rows:
        print(f"   rep {r['rep']}  {r['job']:<22} over {r['dd_over']:>5.0f} / {r['over']:>4}   slots {r['dd_slots']:>5.2f} / {r['slots']:>5.2f}   "
              f"ups {r['ups']}  restarts {r['restarts']}  covers {r['covers']:>4.0f}  {r['path']}")
    by = {(r["rep"], r["job"]): r for r in rows}
    print("\nmeans per job")
    for job in ("builtin", "builtin+lagfix", "builtin+max70", "builtin+max70+lagfix"):
        rs = [by[(k, job)] for k in reps if (k, job) in by]
        f = lambda key: np.nanmean([r[key] for r in rs])
        print(f"   {job:<22} over {f('dd_over'):6.1f} / {f('over'):6.1f}   slots {f('dd_slots'):5.2f} / {f('slots'):5.2f}   ups {f('ups'):.2f}   "
              f"restarts {f('restarts'):.2f}   covers {f('covers'):5.0f}   three or more scale-ups in {sum(r['ups'] >= 3 for r in rs)} of {len(rs)}")
    print("\nbroker-side signal minus default signal, paired within runs (mean [95% t interval], lower in k of n)")
    ps, mac = {}, {}
    for source, key in (("monitoring service", "dd_over"), ("harness", "over")):
        for band, a, b in PAIRS:
            diff = [by[(k, b)][key] - by[(k, a)][key] for k in reps if (k, a) in by and (k, b) in by and not math.isnan(by[(k, a)][key] + by[(k, b)][key])]
            if len(diff) < 2:
                continue
            mean, lo, hi = interval(diff)
            base = np.mean([by[(k, a)][key] for k in reps if (k, a) in by])
            p = float("nan") if no_test else sign_flip_p(diff)
            if not no_test:
                ps[(source, band)] = p
            if source == "monitoring service":
                B = "Stock" if band == "shipped band" else "Target"
                bro = np.mean([by[(k, b)][key] for k in reps if (k, b) in by])
                mac.update({B + "Default": f"{base:.0f}", B + "Broker": f"{bro:.0f}", B + "Diff": f"{mean:.0f}", B + "DiffLo": f"{lo:.0f}",
                            B + "DiffHi": f"{hi:.0f}", B + "Share": f"{-100 * mean / base:.0f}", B + "Lower": str(sum(x < 0 for x in diff)),
                            B + "BrokerMin": f"{min(by[(k, b)][key] for k in reps if (k, b) in by):.0f}",
                            B + "BrokerMax": f"{max(by[(k, b)][key] for k in reps if (k, b) in by):.0f}",
                            B + "DefaultMin": f"{min(by[(k, a)][key] for k in reps if (k, a) in by):.0f}",
                            B + "DefaultMax": f"{max(by[(k, a)][key] for k in reps if (k, a) in by):.0f}"})
            print(f"   seconds over the limit, {source:<18} {band:<18} {mean:+7.1f} [{lo:+7.1f}, {hi:+7.1f}]  {100 * mean / base:+5.0f}% of the default's {base:.0f} s  "
                  f"lower in {sum(x < 0 for x in diff)} of {len(diff)}  " + ("no test" if no_test else f"exact p {p:.4f}")
                  + "  by run " + ", ".join(f"{x:+.0f}" for x in diff))
    for source in ("monitoring service", "harness"):
        p = [ps[(source, band)] for band, _, _ in PAIRS if (source, band) in ps]
        if len(p) == 2:
            order = sorted(range(2), key=lambda i: p[i])
            adj = [0, 0]
            adj[order[0]] = min(1.0, 2 * p[order[0]])
            adj[order[1]] = min(1.0, max(adj[order[0]], p[order[1]]))
            print(f"   Holm over the two tests, {source}: " + ", ".join(f"{band} p = {adj[i]:.4f}" for i, (band, _, _) in enumerate(PAIRS)))
            if source == "monitoring service" and not one_test:
                mac.update({"StockP": f"{adj[0]:.3f}", "TargetP": f"{adj[1]:.3f}"})
    if one_test and ("monitoring service", "shipped band") in ps:
        mac["StockP"] = f"{ps[('monitoring service', 'shipped band')]:.3f}"
        print(f"   one primary test by the plan (shipped band, monitoring service), no correction: exact p = {mac['StockP']}; the Holm lines above do not apply")
    for label, key in (("slots held, monitoring service", "dd_slots"), ("slots in use, harness", "slots"), ("scale-ups of Aggregate", "ups"),
                       ("restarts", "restarts"), ("seconds until the load is covered", "covers")):
        for band, a, b in PAIRS:
            diff = [by[(k, b)][key] - by[(k, a)][key] for k in reps if (k, a) in by and (k, b) in by and not math.isnan(by[(k, a)][key] + by[(k, b)][key])]
            if len(diff) < 2:
                continue
            mean, lo, hi = interval(diff)
            print(f"   {label:<34} {band:<18} {mean:+7.2f} [{lo:+7.2f}, {hi:+7.2f}]  lower in {sum(x < 0 for x in diff)} of {len(diff)}")
            B = "Stock" if band == "shipped band" else "Target"
            name = {"dd_slots": "Slots", "ups": "Ups", "restarts": "Restarts", "covers": "Covers"}.get(key)
            if name:
                fmt = "{:.1f}" if key == "dd_slots" else ("{:.2f}" if key in ("ups", "restarts") else "{:.0f}")
                mac[B + name + "Default"] = fmt.format(np.mean([by[(k, a)][key] for k in reps if (k, a) in by]))
                mac[B + name + "Broker"] = fmt.format(np.mean([by[(k, b)][key] for k in reps if (k, b) in by]))
                mac[B + name + "Diff"] = fmt.format(mean)
                mac[B + name + "DiffLo"] = fmt.format(lo)
                mac[B + name + "DiffHi"] = fmt.format(hi)
                if key == "ups":
                    mac[B + "OneStepBroker"] = str(sum(by[(k, b)]["ups"] == 1 for k in reps if (k, b) in by))
                    mac[B + "ThreeStepsDefault"] = str(sum(by[(k, a)]["ups"] >= 3 for k in reps if (k, a) in by))
    # Per run: the change as a share of the default-signal job's time in the same run, and the extra slots as
    # a share of the default-signal job's slots.
    print("\nby run: change of the time over the limit as a share of the default-signal job's time (monitoring service)")
    for band, a, b in PAIRS:
        B = "Stock" if band == "shipped band" else "Target"
        share = [100 * (by[(k, b)]["dd_over"] - by[(k, a)]["dd_over"]) / by[(k, a)]["dd_over"] for k in reps
                 if (k, a) in by and (k, b) in by and by[(k, a)]["dd_over"] > 0 and not math.isnan(by[(k, b)]["dd_over"])]
        if share:
            print(f"   {band:<18} " + ", ".join(f"{x:+.0f}%" for x in share))
            mac[B + "ShareMin"], mac[B + "ShareMax"] = f"{min(-x for x in share):.0f}", f"{max(-x for x in share):.0f}"
        sa = [by[(k, a)]["dd_slots"] for k in reps if (k, a) in by and (k, b) in by]
        sb = [by[(k, b)]["dd_slots"] for k in reps if (k, a) in by and (k, b) in by]
        if sa and not math.isnan(sum(sa) + sum(sb)):
            mac[B + "SlotsShare"] = f"{100 * (np.mean(sb) - np.mean(sa)) / np.mean(sa):.0f}"
            print(f"   {band:<18} slots held: {np.mean(sa):.1f} -> {np.mean(sb):.1f}, {mac[B + 'SlotsShare']}% more")
    # The band: upper bound at the target minus shipped band, with each signal (not a planned contrast).
    print("\nband at the target minus shipped band, paired within runs, monitoring service (not planned)")
    for signal, a, b in (("Default", "builtin", "builtin+max70"), ("Broker", "builtin+lagfix", "builtin+max70+lagfix")):
        for label, key, name, fmt in (("seconds over the limit", "dd_over", "", "{:.0f}"), ("scale-ups of Aggregate", "ups", "Ups", "{:.2f}")):
            diff = [by[(k, b)][key] - by[(k, a)][key] for k in reps if (k, a) in by and (k, b) in by and not math.isnan(by[(k, a)][key] + by[(k, b)][key])]
            if len(diff) < 2:
                continue
            mean, lo, hi = interval(diff)
            print(f"   {label:<24} {signal.lower():<8} signal {mean:+7.1f} [{lo:+7.1f}, {hi:+7.1f}]  higher in {sum(x > 0 for x in diff)} of {len(diff)}"
                  + ("  by run " + ", ".join(f"{x:+.0f}" for x in diff) if key == "dd_over" else ""))
            mac.update({f"Band{signal}{name}Diff": fmt.format(mean), f"Band{signal}{name}DiffLo": fmt.format(lo),
                        f"Band{signal}{name}DiffHi": fmt.format(hi), f"Band{signal}{name}Higher": str(sum(x > 0 for x in diff))})
    # The maximum of 16 against what the formula asked for (not planned).
    sizes = formula_sizes(runs)
    if sizes:
        firsts, peaks = [x[2] for x in sizes], [x[4] for x in sizes]
        print(f"\nthe formula's size against the maximum of 16, jobs with the broker-side signal ({len(sizes)} job-runs, not planned)")
        print(f"   first size above the maximum while Aggregate ran below it: {min(firsts)} to {max(firsts)} subtasks (median {np.median(firsts):.0f}), "
              f"{min(x[3] for x in sizes):.0f} to {max(x[3] for x in sizes):.0f} s after the rise; by job-run {sorted(firsts)}")
        print(f"   largest size after the rise: {min(peaks)} to {max(peaks)} subtasks (median {np.median(peaks):.0f})")
        mac.update({"FormulaJobRuns": str(len(sizes)), "FormulaFirstMin": str(min(firsts)), "FormulaFirstMax": str(max(firsts)),
                    "FormulaFirstMedian": f"{np.median(firsts):.0f}", "FormulaPeakMin": str(min(peaks)), "FormulaPeakMax": str(max(peaks))})
    if tex:
        mac["Runs"] = str(len(reps))
        with open(tex[0], "w") as f:
            f.write(f"% written by cluster/bench/sigsum.py from {camp}, scenario {scenario}\n")
            for k in sorted(mac):
                value = str(mac[k])
                if value.lstrip("-").isdigit() and len(value.lstrip("-")) > 3:  # 1661 -> 1{,}661, as the text writes thousands
                    value = f"{int(value):,}".replace(",", "{,}")
                f.write(f"\\newcommand{{\\{tex[1]}{k}}}{{{value}}}\n")
        print(f"wrote {len(mac)} macros to {tex[0]}")


if __name__ == "__main__":
    main()
