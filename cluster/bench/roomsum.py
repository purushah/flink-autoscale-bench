"""The decisions at which a model had room above the policy's choice while the bottleneck was saturated.
   python3 roomsum.py <campaign dir, e.g. results-cluster/llm-room> [scenario] [job] [--without N[,N]] [--tally]
An opportunity is a decision of the job for Aggregate inside the overload (between the scenario's two
events) with a busy time of at least 95% at which the policy's choice on the job's own inputs
(rule_choice) is below the largest size on offer. The prompt of the LLM gives an example of when it may
choose more than the policy: such a busy time with a believed demand below the capacity. That narrower
count is printed beside the first. Definition fixed in the plan entry of 24 Nov 2026 19:47 PST, before
the first run. With --tally: one line of counts over all decisions of the job for the bottleneck inside the
overload (how many had a larger size than the policy's on offer, at how many the prompt's condition held,
at how many both, and how often the job chose above the policy)."""
import datetime, glob, json, os, re, sys

BUSY = 0.95
VERTEX = "Aggregate"


def largest_offered(offered):
    """The largest size in an 'offered' field such as 'p3..p16', 'p2,p4,p8' or 'p6'."""
    return max(int(x) for x in re.findall(r"p(\d+)", offered))


def _seconds(ts, t0):
    whole = datetime.datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
    frac = float("0." + (ts[20:].rstrip("Z")[:6] or "0")) if len(ts) > 20 else 0.0
    return whole + frac - t0


def _asked(decisions_path, meta, job):
    """(seconds after the start of the measured phase, the vertex record) of every decision of the job for
    the bottleneck vertex inside the overload at which it was asked to choose."""
    rise, fall = meta["events"][0], meta["events"][1]
    name = "name='bench-" + job.replace("+", "-") + "'"
    with open(decisions_path) as f:
        for line in f:
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if d.get("event") != "decision" or name not in d.get("job", ""):
                continue
            t = _seconds(d["ts"], meta["t0_epoch"])
            if not rise <= t <= fall:
                continue
            for v in (d.get("vertices") or {}).values():
                if v.get("name") == VERTEX and v.get("choice") and v.get("rule_choice") is not None and v.get("offered"):
                    yield t, v


def _condition(v):
    """The example in the LLM's prompt: a busy time of at least 95% and a believed demand below the capacity."""
    return ((v.get("busy") or 0) >= BUSY and v.get("demand") is not None and v.get("capacity_per_subtask") is not None
            and v["demand"] < v["capacity_per_subtask"] * (v.get("current") or 0))


def opportunities(decisions_path, meta, job):
    """The opportunities of one job in one run, in the order they came: seconds after the start of the
    measured phase, the policy's choice, the job's choice, the largest size on offer, whether the job
    chose above the policy, and whether the prompt's own example held."""
    out = []
    for t, v in _asked(decisions_path, meta, job):
        largest = largest_offered(v["offered"])
        if (v.get("busy") or 0) < BUSY or v["rule_choice"] >= largest:
            continue
        choice = int(v["choice"][1:])
        out.append({"t": round(t), "current": v.get("current"), "rule_choice": v["rule_choice"], "choice": choice,
                    "largest": largest, "above": choice > v["rule_choice"], "prompt_example": _condition(v)})
    return out


def tally(decisions_path, meta, job):
    """Counts over the decisions of one job for the bottleneck inside the overload of one run: all of them,
    those with a size above the policy's choice on offer (larger), those at which the prompt's condition
    held (condition), those with both, and how often the job chose above the policy (of all, and of both)."""
    c = {"decisions": 0, "larger": 0, "condition": 0, "both": 0, "chose_larger": 0, "chose_larger_both": 0}
    for _, v in _asked(decisions_path, meta, job):
        larger = v["rule_choice"] < largest_offered(v["offered"])
        cond = _condition(v)
        above = int(v["choice"][1:]) > v["rule_choice"]
        c["decisions"] += 1
        c["larger"] += larger
        c["condition"] += cond
        c["both"] += larger and cond
        c["chose_larger"] += above
        c["chose_larger_both"] += above and larger and cond
    return c


def main():
    argv = sys.argv[1:]
    without = set()
    if "--without" in argv:
        i = argv.index("--without")
        without = {int(x) for x in argv[i + 1].split(",")}
        argv = argv[:i] + argv[i + 2:]
    counts = "--tally" in argv
    argv = [a for a in argv if a != "--tally"]
    camp = argv[0]
    scenario = argv[1] if len(argv) > 1 else "surge"
    job = argv[2] if len(argv) > 2 else "llm"
    runs = sorted((d for d in glob.glob(os.path.join(camp, scenario, "rep*")) if d.rsplit("rep", 1)[1].isdigit()),
                  key=lambda d: int(d.rsplit("rep", 1)[1]))
    if counts:  # one line for a table: runs, decisions, with a larger size on offer, the prompt's condition, both, choices above
        tot, with_larger, with_both, n = {}, 0, 0, 0
        for d in runs:
            if int(d.rsplit("rep", 1)[1]) in without or not os.path.exists(os.path.join(d, "meta.json")):
                continue
            c = tally(os.path.join(d, "decisions.jsonl"), json.load(open(os.path.join(d, "meta.json"))), job)
            n += 1
            with_larger += c["larger"] > 0
            with_both += c["both"] > 0
            for k, v in c.items():
                tot[k] = tot.get(k, 0) + v
        print(f"{camp}, {scenario}, job {job}: runs {n}; decisions for the bottleneck inside the overload {tot.get('decisions', 0)}; "
              f"with a larger size than the policy's on offer {tot.get('larger', 0)} (in {with_larger} runs); the prompt's condition held in "
              f"{tot.get('condition', 0)}; both in {tot.get('both', 0)} (in {with_both} runs); chose above the policy in "
              f"{tot.get('chose_larger', 0)}, of these with both {tot.get('chose_larger_both', 0)}")
        return
    total, above, example, example_above, with_one, n = 0, 0, 0, 0, 0, 0
    for d in runs:
        rep = int(d.rsplit("rep", 1)[1])
        if rep in without or not os.path.exists(os.path.join(d, "meta.json")):
            continue
        meta = json.load(open(os.path.join(d, "meta.json")))
        ops = opportunities(os.path.join(d, "decisions.jsonl"), meta, job)
        n += 1
        with_one += bool(ops)
        total += len(ops)
        above += sum(o["above"] for o in ops)
        example += sum(o["prompt_example"] for o in ops)
        example_above += sum(o["prompt_example"] and o["above"] for o in ops)
        steps = ", ".join(f"{o['t'] - meta['events'][0]} s after the rise: at {o['current']}, policy {o['rule_choice']}, chose {o['choice']} of up to {o['largest']}"
                          + (" (prompt's example)" if o["prompt_example"] else "") for o in ops)
        print(f"   rep {rep}  {job}: {len(ops)} opportunities" + (": " + steps if ops else ""))
    left = (" (left out by --without: " + ", ".join(map(str, sorted(without))) + ")") if without else ""
    print(f"{camp}, {scenario}, job {job}: {n} runs{left}; runs with an opportunity {with_one}; opportunities {total}, "
          f"chose above the policy in {above}; of these the prompt's own example held in {example}, above in {example_above}")


if __name__ == "__main__":
    main()
