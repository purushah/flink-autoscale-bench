"""The decisions at which a model had room above the policy's choice while the bottleneck was saturated.
   python3 roomsum.py <campaign dir, e.g. results-cluster/llm-room> [scenario] [job] [--without N[,N]]
An opportunity is a decision of the job for Aggregate inside the overload (between the scenario's two
events) with a busy time of at least 95% at which the policy's choice on the job's own inputs
(rule_choice) is below the largest size on offer. The prompt of the LLM gives an example of when it may
choose more than the policy: such a busy time with a believed demand below the capacity. That narrower
count is printed beside the first. Definition fixed in the plan entry of 24 Nov 2026 19:47 PST, before
the first run."""
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


def opportunities(decisions_path, meta, job):
    """The opportunities of one job in one run, in the order they came: seconds after the start of the
    measured phase, the policy's choice, the job's choice, the largest size on offer, whether the job
    chose above the policy, and whether the prompt's own example held."""
    rise, fall = meta["events"][0], meta["events"][1]
    out = []
    with open(decisions_path) as f:
        for line in f:
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if d.get("event") != "decision" or f"name='bench-{job}'" not in d.get("job", ""):
                continue
            t = _seconds(d["ts"], meta["t0_epoch"])
            if not rise <= t <= fall:
                continue
            for v in (d.get("vertices") or {}).values():
                if v.get("name") != VERTEX or not v.get("choice") or v.get("rule_choice") is None or not v.get("offered"):
                    continue
                largest = largest_offered(v["offered"])
                if (v.get("busy") or 0) < BUSY or v["rule_choice"] >= largest:
                    continue
                choice = int(v["choice"][1:])
                out.append({"t": round(t), "current": v.get("current"), "rule_choice": v["rule_choice"], "choice": choice,
                            "largest": largest, "above": choice > v["rule_choice"],
                            "prompt_example": v.get("demand") is not None and v.get("capacity_per_subtask") is not None
                            and v["demand"] < v["capacity_per_subtask"] * (v.get("current") or 0)})
    return out


def main():
    argv = sys.argv[1:]
    without = set()
    if "--without" in argv:
        i = argv.index("--without")
        without = {int(x) for x in argv[i + 1].split(",")}
        argv = argv[:i] + argv[i + 2:]
    camp = argv[0]
    scenario = argv[1] if len(argv) > 1 else "surge"
    job = argv[2] if len(argv) > 2 else "llm"
    runs = sorted((d for d in glob.glob(os.path.join(camp, scenario, "rep*")) if d.rsplit("rep", 1)[1].isdigit()),
                  key=lambda d: int(d.rsplit("rep", 1)[1]))
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
