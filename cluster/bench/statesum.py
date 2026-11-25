"""The backlog signal on a job with real state (queue_state.sh): broker-side signal against default
signal, both with the built-in rule as shipped, paired within runs.
   python3 statesum.py <campaign dir, e.g. results-cluster/state> [scenario] [--without N[,N]] [--tex <file> <macro prefix>]
Per run and job, from the harness's files: the seconds over the latency limit by the laptop's definition
(a second whose 95th percentile is above 2 s on any subtask; a second without output takes the second
before it for up to three seconds and is over the limit after that), which is the measure of this test,
because a job that restores a state emits nothing for a long time; the time lost to restarts (seconds
without output in stretches of three or more); scale-ups of Aggregate and rescales (Aggregate or Measure
changed size, each a restart; counted from the samples in which the job is RUNNING), the seconds
until the load is covered (as sigsum.py) and the records that left the job in the measured phase. From the monitoring service (<campaign dir>/datadog.json, as
sigsum.py): the counter of seconds over the limit, which counts no second without output, and the slots
held. A run counts when meta.json says that it began drained and
settled, with both jobs at the scenario's size and the state in a completed checkpoint of each; the
others are listed and left out. With --without the runs with these numbers are left out as well (runs
that the cluster disturbed, marked by disturbed_cluster.py before any outcome was read)."""
import csv, glob, json, math, os, sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigsum import collapse, harness, interval, sign_flip_p  # noqa: E402

STOP_S = 3  # a stretch without output of at least this many seconds is a stop (a restart), not a quiet second
LIMIT_MS = 2000.0
FILL_S = 3  # a second without output takes the latency of the second before it for up to this many seconds


def pair(jobs):
    """(default, broker): the job without and the job with the broker-side signal (+lagfix)."""
    with_fix = [j for j in jobs if "lagfix" in j.split("+")]
    without = [j for j in jobs if "lagfix" not in j.split("+")]
    if len(jobs) != 2 or len(with_fix) != 1 or len(without) != 1:
        raise ValueError(f"expected one job with +lagfix and one without, got {jobs}")
    return without[0], with_fix[0]


def _rows(latency_csv):
    """The rows of a latency file. Zero bytes are dropped: when a job still emits at the end of a run, the
    harness can rewrite the file under the collector, which then writes at its old position and leaves a
    block of zero bytes before rows whose times are still epoch seconds (ISSUES.md #177). Those rows lie
    after the measured phase and no caller counts them."""
    with open(latency_csv, newline="") as f:
        yield from csv.DictReader(line.replace("\0", "") for line in f)


def _quiet(latency_csv, job, duration):
    """Per whole second of the measured phase: True when the job emitted nothing in it."""
    out = set()
    for r in _rows(latency_csv):
        if r["job"] == job and 0 <= float(r["t"]) < duration:
            out.add(int(float(r["t"])))
    return [s not in out for s in range(int(duration))]


def late_seconds(latency_csv, job, duration, limit=LIMIT_MS):
    """Seconds of the measured phase over the latency limit, as bench/analyze.py counts them on the laptop:
    the worst subtask's 95th percentile of the second is above the limit, or the job emitted nothing in
    the second and either the last second with output was above the limit or it lies more than FILL_S
    seconds back."""
    worst = {}
    for r in _rows(latency_csv):
        if r["job"] == job and 0 <= float(r["t"]) < duration:
            sec = int(float(r["t"]))
            worst[sec] = max(worst.get(sec, 0.0), float(r["p95_ms"]))
    late, last, since = 0, None, 0
    for sec in range(int(duration)):
        if sec in worst:
            last, since = worst[sec], 0
        else:
            since += 1
        if last is None or since > FILL_S or last > limit:
            late += 1
    return late


def down_seconds(latency_csv, job, duration):
    """Whole seconds of the measured phase in which the job emitted nothing."""
    return sum(_quiet(latency_csv, job, duration))


def emitted(latency_csv, job, duration):
    """Records that left the job in the measured phase (the count column of the latency file)."""
    return sum(int(float(r["count"])) for r in _rows(latency_csv) if r["job"] == job and 0 <= float(r["t"]) < duration)


def down_stretches(latency_csv, job, duration, at_least=STOP_S):
    """Lengths of the stretches of at least `at_least` seconds without output, in the order they happened."""
    out, run = [], 0
    for quiet in _quiet(latency_csv, job, duration) + [False]:
        if quiet:
            run += 1
            continue
        if run >= at_least:
            out.append(run)
        run = 0
    return out


def rescales(samples_csv, job, duration):
    """Scale-ups of Aggregate, rescales (Aggregate or Measure changed size; each is a restart) and the
    sizes of Aggregate in the measured phase, from the samples in which the job is RUNNING. A job with
    state takes long enough to restart that the samples catch it as CREATED, with the parallelism it was
    submitted with: that is not a size it ran at."""
    agg, both = [], []
    with open(samples_csv, newline="") as f:
        for r in csv.DictReader(f):
            if r["job"] != job or r.get("state") != "RUNNING" or not r.get("p_aggregate") or not r.get("p_measure"):
                continue
            t = float(r["t"])
            if 0 <= t <= duration:
                agg.append((t, int(float(r["p_aggregate"]))))
                both.append((t, (int(float(r["p_aggregate"])), int(float(r["p_measure"])))))
    ch = collapse(agg)
    return {"ups": sum(1 for c in ch if c[2] > c[1]), "rescales": len(collapse(both)),
            "path": "->".join([str(ch[0][1])] + [str(c[2]) for c in ch]) if ch else (str(agg[0][1]) if agg else "")}


def started_ready(m):
    """Whether the run began as the plan asks, and what was missing if not. A run of jobs without state
    has no state block in meta.json and nothing to fill."""
    start, state = m.get("start") or {}, m.get("state")
    checks = [("drained", m.get("drained")), ("settled", m.get("settled")),
              ("at the scenario's size", start.get("at_scenario_size")), ("equal sizes", start.get("equal"))]
    if state is not None:
        checks.append(("state filled", state.get("filled")))
    missing = [name for name, ok in checks if not ok]
    return not missing, missing


def state_sizes(m):
    """(GiB of state the jobs are to hold, job -> GiB in its last completed checkpoint when the measured
    phase began). Zeros for a run of jobs without state."""
    state = m.get("state") or {}
    wanted, held = state.get("wanted") or {}, state.get("bytes") or {}
    return (max(wanted.values()) / 2**30 if wanted else 0.0), {j: (held.get(j) or 0) / 2**30 for j in m["jobs"]}


def main():
    argv = sys.argv[1:]
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
    dd = {}
    if os.path.exists(os.path.join(camp, "datadog.json")):
        dd = json.load(open(os.path.join(camp, "datadog.json"))).get("runs", {})
    rows, unready, names, gib = [], [], None, None
    for d in runs:
        rep = int(d.rsplit("rep", 1)[1])
        if rep in without or not os.path.exists(os.path.join(d, "meta.json")):
            continue
        m, h, need = harness(d)
        ok, missing = started_ready(m)
        if not ok:
            unready.append((rep, missing))
            continue
        names = pair(m["jobs"])
        gib, held = state_sizes(m)
        for job, v in h.items():
            x = dd.get(f"{scenario}/{rep}", {}).get(job)
            stops = down_stretches(os.path.join(d, "latency.csv"), job, m["duration"])
            v.update(rescales(os.path.join(d, "samples.csv"), job, m["duration"]))  # in place of sigsum's ups, restarts and path
            v.update(rep=rep, job=job, dd_over=x[0] if x else float("nan"), dd_slots=(x[2] - x[3]) if x else float("nan"),
                     dd_measured=x[1] if x else float("nan"), late=late_seconds(os.path.join(d, "latency.csv"), job, m["duration"]),
                     down=sum(stops), stops=stops, quiet=down_seconds(os.path.join(d, "latency.csv"), job, m["duration"]),
                     emitted=emitted(os.path.join(d, "latency.csv"), job, m["duration"]) / 1000.0, sent=(m.get("records_sent") or 0) / 1000.0,
                     state_gib=held[job])
            rows.append(v)
    reps = sorted({r["rep"] for r in rows})
    left = (" (left out by --without: " + ", ".join(map(str, sorted(without))) + ")") if without else ""
    print(f"{camp}, {scenario}: {len(reps)} runs counted{left}")
    for rep, missing in unready:
        print(f"   rep {rep} is not counted: it began without {', '.join(missing)}")
    if not rows:
        return
    a, b = names
    print("per run: state in the last checkpoint at the start (GiB), seconds over the limit (the test's measure; then the monitoring service's counter "
          "of its seconds measured), slots held (service), scale-ups of Aggregate, rescales, seconds without output in stops of 3 s or more (and the "
          "stops), seconds until the load is covered, sizes of Aggregate")
    for r in rows:
        print(f"   rep {r['rep']}  {r['job']:<22} state {r['state_gib']:4.2f}  over {r['late']:>4}  (service {r['dd_over']:>4.0f} of {r['dd_measured']:>4.0f})   slots {r['dd_slots']:>5.2f}   "
              f"ups {r['ups']}  rescales {r['rescales']}  down {r['down']:>4} {r['stops']}  covers {r['covers']:>4.0f}  {r['path']}  "
              f"emitted {r['emitted']:.0f}k of {r['sent']:.0f}k")
    by = {(r["rep"], r["job"]): r for r in rows}
    mean = lambda job, key: float(np.nanmean([by[(k, job)][key] for k in reps]))
    print("\nmeans per job")
    for job in (a, b):
        print(f"   {job:<22} over {mean(job, 'late'):6.1f}  (service {mean(job, 'dd_over'):6.1f})   slots {mean(job, 'dd_slots'):5.2f}   ups {mean(job, 'ups'):.2f}   "
              f"rescales {mean(job, 'rescales'):.2f}   down {mean(job, 'down'):6.1f}   covers {mean(job, 'covers'):5.0f}   emitted {mean(job, 'emitted'):.0f}k")
    every_stop = [s for r in rows for s in r["stops"]]
    mac = {"Runs": str(len(reps)), "Gib": f"{gib:.0f}"}
    n_rescales = sum(r["rescales"] for r in rows)
    if every_stop and n_rescales:
        per = sum(every_stop) / n_rescales
        print(f"   without output: {sum(every_stop)} s in {len(every_stop)} stops of {min(every_stop)} to {max(every_stop)} s, over {n_rescales} rescales: {per:.1f} s per rescale")
        mac.update({"DownPerRescale": f"{per:.0f}", "StopMin": str(min(every_stop)), "StopMax": str(max(every_stop))})
    print("\nbroker-side signal minus default signal, paired within runs (mean [95% t interval], lower in k of n)")
    for label, key, name, digits in (("seconds over the limit", "late", "Over", 0), ("the monitoring service's counter", "dd_over", "Counter", 0),
                                     ("slots held, monitoring service", "dd_slots", "Slots", 2), ("scale-ups of Aggregate", "ups", "Ups", 2),
                                     ("rescales", "rescales", "Rescales", 2), ("seconds without output in stops", "down", "Down", 0),
                                     ("seconds until the load is covered", "covers", "Covers", 0),
                                     ("records emitted, thousands", "emitted", "Emitted", 0)):
        diff = [by[(k, b)][key] - by[(k, a)][key] for k in reps if not math.isnan(by[(k, a)][key] + by[(k, b)][key])]
        if len(diff) < 2:
            print(f"   {label:<44} fewer than two pairs")
            continue
        m_, lo, hi = interval(diff)
        base = float(np.nanmean([by[(k, a)][key] for k in reps]))
        line = f"   {label:<44} {m_:+8.2f} [{lo:+8.2f}, {hi:+8.2f}]  lower in {sum(x < 0 for x in diff)} of {len(diff)}"
        if key == "late":  # the one planned test
            p = sign_flip_p(diff)
            line += f"  {100 * m_ / base:+5.0f}% of the default's {base:.0f} s  exact p {p:.4f}"
            mac.update({"OverP": f"{p:.3f}", "OverShare": f"{-100 * m_ / base:.0f}", "OverLower": str(sum(x < 0 for x in diff))})
        print(line + "  by run " + ", ".join(f"{x:+.{digits}f}" for x in diff))
        if name:
            f = f"{{:.{digits}f}}"
            mac.update({name + "Default": f.format(mean(a, key)), name + "Broker": f.format(mean(b, key)), name + "Diff": f.format(m_),
                        name + "DiffLo": f.format(lo), name + "DiffHi": f.format(hi)})
    if tex:
        with open(tex[0], "w") as out:
            out.write(f"% Generated by cluster/bench/statesum.py from {camp} ({scenario}). Do not edit.\n")
            for k in sorted(mac):
                out.write(f"\\newcommand{{\\{tex[1]}{k}}}{{{mac[k]}}}\n")
        print(f"\nwrote {len(mac)} macros to {tex[0]}")


if __name__ == "__main__":
    main()
