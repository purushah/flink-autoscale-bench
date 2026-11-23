"""Replay of the size a scale-down keeps for an earlier level, on recorded on-off runs (ISSUES.md #119,
second cause). For every evaluation of a job with the look-back in the decision log it recomputes the
policy's choice for Aggregate from the logged readings in two forms:
  as built   keep = max(size, ceil(highest demand of the window / target utilization / capacity))
  with margin  the same with the highest demand times (1 + margin): the capacity kept must carry any
               demand that still counts as that level (the cap's margin, 10%)
and scales down only when that frees at least two subtasks and MINSLOTS slots, among the parallelisms
the evaluation offered. It counts how often the "as built" form reproduces the logged choice. The replay is not a run: the recorded job is at the
size the built form gave it, so later readings are not those of a job that held.
   python3 keepreplay.py <run dir>...             jobs whose name ends in -c (built without the margin)
   python3 keepreplay.py --job d <run dir>...     jobs whose name ends in -d, which run WITH the margin
                                                  (scale-down.window-keeps-margin, Test D): it counts how
                                                  often the form with the margin reproduces the logged
                                                  choice, and what the form without it would have done"""
import calendar, json, math, os, sys, time

TARGET, MARGIN, MINSLOTS = 0.7, 0.10, 2
ALIGN = 240  # key groups of Aggregate at scale 2: the built-in rule's alignment allows 8, 10, 12, 15, 16, ...


def epoch(ts):
    whole, _, frac = ts.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


def options(a):
    """The parallelisms offered at this evaluation: the logged range, and inside it the values that
    divide the key groups evenly (ALIGN), plus the current one, as DecisionEngine.options()."""
    lo, _, hi = a.get("offered", "").replace("p", "").partition("..")
    if not lo.isdigit() or not hi.isdigit():
        return [a["current"]]
    return [p for p in range(int(lo), int(hi) + 1) if p == a["current"] or ALIGN % p == 0]


def fit(opts, wanted):
    for p in opts:
        if p >= wanted:
            return p
    return opts[-1]


def keep(a, margin):
    peak = a.get("peak_rate")
    kept = 0
    if peak and peak > 0:
        kept = max(1, math.ceil(peak * (1 + margin) / TARGET / a["capacity_per_subtask"] - 1e-9))
    return max(a["rule_size"], kept)


def choice(a, margin):
    """DecisionEngine.ruleChoice with the kept level raised by the margin, then the two-slot rule."""
    cur, opts = a["current"], options(a)
    if cur < a["rule_trigger"]:
        return fit(opts, a["rule_size"]), fit(opts, a["rule_size"])
    k = keep(a, margin)
    down = fit(opts, k)
    policy = down if cur - k >= 2 and down < cur else cur
    applied = cur if policy < cur and cur - policy < MINSLOTS else policy
    return policy, applied


def main_with_margin(dirs, suffix):
    """Jobs that run with the margin: the built form is choice(a, MARGIN)."""
    total = {"evaluations": 0, "policy choice as logged": 0, "applied as logged": 0, "scale-downs applied": 0,
             "evaluations where the form without the margin scales down and this one holds": 0,
             "scale-ups of Aggregate ordered": 0, "of them with the demand within the margin of the level": 0}
    for d in dirs:
        meta = json.load(open(os.path.join(d, "meta.json")))
        t0, events = meta["t0_epoch"], meta.get("events") or []
        later = t0 + (events[2] if len(events) > 2 else 0)  # second rise
        print(d)
        for line in open(os.path.join(d, "decisions.jsonl"), errors="replace"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if f"-{suffix}'" not in r.get("job", "") or "rule" != r.get("model") or not isinstance(r.get("vertices"), dict):
                continue
            a = next((v for v in r["vertices"].values() if isinstance(v, dict) and v.get("name") == "Aggregate"), None)
            if a is None or "rule_size" not in a or "peak_rate" not in a or "rule_choice" not in a:
                continue
            t = epoch(r["ts"])
            if t < later:
                continue
            total["evaluations"] += 1
            (bp, ba), (mp, ma) = choice(a, 0.0), choice(a, MARGIN)
            total["policy choice as logged"] += mp == a["rule_choice"]
            total["applied as logged"] += ma == a["target"]
            if mp != a["rule_choice"] or ma != a["target"]:
                print(f"   {t - t0:6.0f} s  NOT AS LOGGED: replay {mp} / {ma}, log {a['rule_choice']} / {a['target']} "
                      f"(current {a['current']}, size {a['rule_size']}, trigger {a['rule_trigger']}, offered {a.get('offered')})")
            level = a["peak_rate"] / TARGET / a["capacity_per_subtask"] if a["peak_rate"] else 0.0
            if a["target"] < a["current"]:
                total["scale-downs applied"] += 1
                print(f"   {t - t0:6.0f} s  {a['current']:2d} -> {a['target']:2d}   (level {a['peak_rate']:.0f}, capacity "
                      f"{a['capacity_per_subtask']:.0f}: needs {level:.2f}, with the margin {level * (1 + MARGIN):.2f}; "
                      f"without the margin the policy would go to {ba}; offered {a.get('offered')})")
            elif ba < a["current"] and ma == a["current"]:
                total["evaluations where the form without the margin scales down and this one holds"] += 1
            if a["target"] > a["current"]:
                total["scale-ups of Aggregate ordered"] += 1
                # as flaporders.py counts it for Test B (B5): the demand is not above the earlier level plus 10%
                prior, demand = a.get("prior_peak_rate"), a.get("demand")
                within = prior is not None and demand is not None and demand <= prior * (1 + MARGIN)
                total["of them with the demand within the margin of the level"] += within
                print(f"   {t - t0:6.0f} s  {a['current']:2d} -> {a['target']:2d} UP  (demand {demand}, earlier level {prior}, "
                      f"capacity {a['capacity_per_subtask']:.0f}; within the margin: {within})")
    print(f"all runs given, from the second rise on, jobs ending in -{suffix} (built with the margin):", total)


def main():
    args = sys.argv[1:]
    if args[:1] == ["--job"]:
        suffix, args = args[1], args[2:]
        if suffix != "c":
            return main_with_margin(args, suffix)
    total = {"evaluations": 0, "policy choice as logged": 0, "applied as logged": 0, "scale-downs as built": 0,
             "of them with the margin: same": 0, "shallower": 0, "held": 0, "new with the margin": 0}
    for d in args:
        meta = json.load(open(os.path.join(d, "meta.json")))
        t0, events = meta["t0_epoch"], meta.get("events") or []
        later = t0 + (events[2] if len(events) > 2 else 0)  # second rise
        print(d)
        for line in open(os.path.join(d, "decisions.jsonl"), errors="replace"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if "-c'" not in r.get("job", "") or "rule" != r.get("model") or not isinstance(r.get("vertices"), dict):
                continue
            a = next((v for v in r["vertices"].values() if isinstance(v, dict) and v.get("name") == "Aggregate"), None)
            if a is None or "rule_size" not in a or "peak_rate" not in a or "rule_choice" not in a:
                continue
            t = epoch(r["ts"])
            if t < later:
                continue
            total["evaluations"] += 1
            (bp, ba), (mp, ma) = choice(a, 0.0), choice(a, MARGIN)
            total["policy choice as logged"] += bp == a["rule_choice"]
            total["applied as logged"] += ba == a["target"]
            level = a["peak_rate"] / TARGET / a["capacity_per_subtask"]
            if ba < a["current"]:
                total["scale-downs as built"] += 1
                kind = "held" if ma == a["current"] else "same" if ma == ba else "shallower"
                total["of them with the margin: same" if kind == "same" else kind] += 1
                print(f"   {t - t0:6.0f} s  {a['current']:2d} -> {ba:2d} as built; with the margin: "
                      f"{'held at ' + str(ma) if kind == 'held' else 'to ' + str(ma)}   (level {a['peak_rate']:.0f}, capacity "
                      f"{a['capacity_per_subtask']:.0f}: needs {level:.2f}, with the margin {level * (1 + MARGIN):.2f}; offered {a.get('offered')})")
            elif ma < a["current"]:
                total["new with the margin"] += 1
    print("all runs given, from the second rise on:", total)


if __name__ == "__main__":
    main()
