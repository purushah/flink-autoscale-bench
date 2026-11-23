"""Flapping-load runs: late seconds in the first cycle and after it, scaling actions of Aggregate after
the first cycle (one-sample readings during a restart are dropped), undone scale-downs, slots, and after
the first cycle the restarts for a change of any operator (how many of them changed the sink only) and
the slots of the job's TaskManager pods (4 per pod).
   python3 flapsum.py <run dir>..."""
import collections, csv, json, os, re, statistics as st, sys, calendar, time

LIMIT = 2000.0
UNDO = 300

def epoch(ts):
    whole, _, frac = ts.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)

def collapse(p):
    """p: [(t, size)] samples. Returns changes [(t, a, b)] with states that lasted under 12 s dropped."""
    runs = []  # [start, end, size]
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

def run(d):
    m = json.load(open(os.path.join(d, "meta.json")))
    dur, t0 = m["duration"], m["t0_epoch"]
    ev = m["events"]
    cut = ev[2]  # start of the second on-phase
    lat = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(open(os.path.join(d, "latency.csv"))):
        t = float(r["t"])
        if 0 <= t <= dur:
            lat[r["job"]][int(t)].append(float(r["p95_ms"]))
    par = collections.defaultdict(list)
    both = collections.defaultdict(list)
    pods = collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(d, "samples.csv"))):
        if r.get("tm_pods"):
            pods[r["job"]].append((float(r["t"]), 4 * int(float(r["tm_pods"]))))
        if r.get("p_aggregate"):
            sizes = [float(r[k]) for k in ("p_source", "p_aggregate", "p_measure") if r.get(k)]
            par[r["job"]].append((float(r["t"]), int(float(r["p_aggregate"])), max(sizes)))
            if r.get("p_measure"):
                both[r["job"]].append((float(r["t"]), (int(float(r["p_aggregate"])), int(float(r["p_measure"])))))
    dec = collections.defaultdict(list)
    for line in open(os.path.join(d, "decisions.jsonl")):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        j = re.search(r"name='bench-([^']+)'", o.get("job", ""))
        if j and o.get("event") == "decision":
            o["t"] = epoch(o["ts"]) - t0
            dec[j.group(1)].append(o)
    out = {}
    for job in m["jobs"]:
        sec = {s: max(v) for s, v in lat[job].items()}
        over = [s for s, v in sec.items() if v > LIMIT]
        p = par[job]
        ch = [c for c in collapse([(x[0], x[1]) for x in p]) if 0 <= c[0] <= dur]
        late = [c for c in ch if c[0] >= cut]
        downs = [c for c in late if c[2] < c[1]]
        undone = [c for i, c in enumerate(late) if c[2] < c[1]
                  and any(n[2] > n[1] and n[0] - c[0] <= UNDO for n in late[i + 1:])]
        inrun = [x for x in p if 0 <= x[0] <= dur]
        anyop = [c for c in collapse(both[job]) if cut <= c[0] <= dur]
        sink_only = [c for c in anyop if c[1][0] == c[2][0]]
        podslots = [x[1] for x in pods[job] if cut <= x[0] <= dur]
        after = [x for x in p if cut <= x[0] <= dur]
        ds = dec.get(job.replace("+", "-"), [])
        inr = [o for o in ds if 0 <= o["t"] <= dur]
        calls = [o["latency_ms"] for o in inr if o.get("model") and o.get("status") == "DECIDED" and o.get("latency_ms")]
        above = below = same = 0
        for o in inr:
            if o.get("status") != "DECIDED" or not o.get("model"):
                continue
            for v in o.get("vertices", {}).values():
                if v.get("rule_choice") is None:
                    continue
                above += v["target"] > v["rule_choice"]; below += v["target"] < v["rule_choice"]; same += v["target"] == v["rule_choice"]
        out[job] = dict(over=len(over), first=sum(s < cut for s in over), rest=sum(s >= cut for s in over),
                        actions=len(late), downs=len(downs), undone=len(undone),
                        restarts=len(anyop), sink_only=len(sink_only),
                        podslots=st.mean(podslots) if podslots else float("nan"),
                        slots=st.mean(x[2] for x in inrun), slots_rest=st.mean(x[2] for x in after),
                        min_rest=min(x[1] for x in after), status=dict(collections.Counter(o.get("status") for o in inr)),
                        call_med=st.median(calls) if calls else None, call_max=max(calls) if calls else None,
                        tok_in=st.median([o["input_tokens"] for o in inr if o.get("input_tokens")]) if any(o.get("input_tokens") for o in inr) else None,
                        tok_out=st.median([o["output_tokens"] for o in inr if o.get("output_tokens")]) if any(o.get("output_tokens") for o in inr) else None,
                        same=same, above=above, below=below, changes=late)
    return m, out

KNOWN = ["builtin+max70", "rule", "rule+b", "rule+c", "llm", "llm+b", "llm+c"]
ORDER = []
allr = []
for d in sys.argv[1:]:
    m, out = run(d)
    allr.append((m, out))
    # the jobs of the runs given, in a fixed order; a job must be in every run to get a mean
    ORDER = [j for j in KNOWN if j in out] + [j for j in m["jobs"] if j not in KNOWN]
    print(f"{m['scenario']} rep {m['rep']}  cut at {m['events'][2]:.0f} s")
    print("   job            over first  rest  actions downs undone  restarts sink  slots  slots after  pod slots after  min   decisions / calls")
    for j in ORDER:
        o = out[j]
        extra = ""
        if o["call_med"]:
            extra = f"calls median {o['call_med']:.0f} max {o['call_max']:.0f} ms, tokens {o['tok_in']:.0f}/{o['tok_out']:.0f}; same {o['same']} above {o['above']} below {o['below']}"
        print(f"   {j:<14} {o['over']:>4} {o['first']:>5} {o['rest']:>5}  {o['actions']:>7} {o['downs']:>5} {o['undone']:>6}  {o['restarts']:>8} {o['sink_only']:>4}  {o['slots']:>5.2f}  {o['slots_rest']:>11.2f}  {o['podslots']:>15.2f}  {o['min_rest']:>3}   {o['status']} {extra}")
    for j in ORDER:
        print(f"      {j:<14} " + ", ".join(f"{t:.0f}s {a}->{b}" for t, a, b in out[j]["changes"]))
    print()
if len(allr) > 1:
    print("means over the runs given")
    for j in ORDER:
        f = lambda k: st.mean(o[j][k] for _, o in allr)
        print(f"   {j:<14} over {f('over'):6.1f}  first {f('first'):6.1f}  rest {f('rest'):5.1f} ({', '.join(str(o[j]['rest']) for _, o in allr)})  actions {f('actions'):4.1f}  downs {f('downs'):4.1f}  undone {f('undone'):4.1f}  restarts {f('restarts'):4.1f} ({', '.join(str(o[j]['restarts']) for _, o in allr)})  sink only {f('sink_only'):3.1f}  slots {f('slots'):5.2f}  after {f('slots_rest'):5.2f}  pod slots after {f('podslots'):5.2f}")
