#!/usr/bin/env python3
"""Offline check: do models reproduce the built-in autoscaler's parallelism formula?

Each case is a fixed set of settings and measurements for one vertex. The reference answer is
computed in code with the built-in formula. The same case is then put to:

  jev-raw       Jev, given only the rule and the raw measurements (options "1..8 subtasks")
  jev-computed  Jev, with each option's capacity/utilization/verdict worked out in code
                (this is what the operator plugin does)
  haiku / sonnet / opus / fable   Claude models, given the rule and the raw measurements

Keys are read from the environment only: TYPESAFE_API_KEY (Jev), ANTHROPIC_API_KEY (Claude).

  python3 bench/model_eval.py run --models jev-raw,jev-computed --repeats 10
  python3 bench/model_eval.py run --models haiku,sonnet,opus,fable --repeats 5
  python3 bench/model_eval.py report

A larger randomized suite (60 cases in six equally sized groups) is used for the paper:

  python3 bench/model_eval.py --suite random run --models jev-raw,jev-computed,haiku,sonnet,opus,fable --repeats 3
"""
import argparse
import collections
import concurrent.futures as cf
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request
from fractions import Fraction as F
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "results" / "model_eval"

SETTINGS = {
    "target_utilization_pct": 60,
    "min_parallelism": 1,
    "max_parallelism": 8,
    "backlog_catch_up_seconds": 60,
}


def case(cid, title, p, arriving, processed, busy, backlog=0, tests="", scored=True, per_subtask_busy=None):
    return dict(id=cid, title=title, parallelism=p, arriving=arriving, processed=processed, busy=busy,
                backlog=backlog, tests=tests, scored=scored, per_subtask_busy=per_subtask_busy)


CASES = [
    case("c01", "Steady, on target", 4, 1200, 300, "0.60", tests="leaves a healthy job alone"),
    case("c02", "Clear overload", 2, 1500, 500, "1.00", tests="jumps to the right size in one step"),
    case("c03", "Slightly over target", 4, 1400, 350, "0.70", tests="acts on a small miss"),
    case("c04", "Clearly oversized", 8, 600, 75, "0.15", tests="scales down, and far enough"),
    case("c05", "Slightly oversized", 5, 1250, 250, "0.50", tests="avoids a pointless restart"),
    case("c06", "Over target with backlog", 4, 1400, 350, "0.70", backlog=30000, tests="accounts for backlog"),
    case("c07", "Demand beyond the limit", 4, 3000, 500, "1.00", tests="respects the maximum"),
    case("c08", "Worker capacity halved", 4, 800, 200, "0.80", tests="uses measured capacity"),
    case("c09", "Rounding boundary, 900 rec/s", 2, 900, 450, "0.90", tests="exact arithmetic"),
    case("c10", "Rounding boundary, 901 rec/s", 2, 901, "450.5", "0.901", tests="exact arithmetic"),
    case("c11", "Skew (not scored)", 4, 1100, 275, "0.55", scored=False,
         per_subtask_busy=[1.0, 0.4, 0.4, 0.4], tests="formula only sees the average"),
]


GROUPS = ["scale up", "scale up with backlog", "hold", "scale down", "limited by the maximum", "rounding boundary"]


def random_cases(n=60, seed=7):
    """n cases in equal groups by what the formula does with them. Values are chosen so that the
    measurements are exact decimals (capacity in steps of 50 rec/s, busy time in whole percent)."""
    rng = random.Random(seed)
    lo, hi = SETTINGS["min_parallelism"], SETTINGS["max_parallelism"]
    target = F(SETTINGS["target_utilization_pct"], 100)
    buckets = {g: [] for g in GROUPS}
    while any(len(b) < n // len(GROUPS) for b in buckets.values()):
        p = rng.randint(lo, hi)
        cap = rng.randrange(100, 1001, 50)
        overloaded = rng.random() < 0.3
        busy = F(100 if overloaded else rng.randint(10, 99), 100)
        processed = cap * busy  # per subtask
        # A backpressured vertex is offered more than it can process.
        arriving = int(processed * p * (F(rng.randint(105, 300), 100) if overloaded else 1))
        if not overloaded:
            processed = F(arriving, p)
            busy = processed / cap
            if busy.denominator > 1000 or (busy * 1000) % 10:  # keep busy time at whole percent
                continue
        backlog = rng.choice([0, 0, 0, rng.randrange(5000, 200001, 5000)])
        c = case("", "", p, arriving, str(float(processed)), str(float(busy)), backlog=backlog)
        if F(str(c["processed"])) != processed or F(str(c["busy"])) != busy:
            continue
        exact = (needed_rate(c) / target) / (capacity_per_subtask(c) * p) * p  # before rounding and clamping
        want = formula(c)
        frac = exact - math.floor(exact)
        if lo < exact <= hi and (frac <= F(3, 100) or frac >= F(97, 100)) and frac != 0:
            g = "rounding boundary"
        elif exact > hi:
            g = "limited by the maximum"
        elif want > p:
            g = "scale up with backlog" if backlog else "scale up"
        elif want == p:
            g = "hold"
        else:
            g = "scale down"
        if len(buckets[g]) < n // len(GROUPS):
            buckets[g].append(c)
    out = []
    for g in GROUPS:
        for c in buckets[g]:
            c["id"] = "r%02d" % (len(out) + 1)
            c["title"] = c["tests"] = g
            out.append(c)
    return out


def capacity_per_subtask(c):
    return F(str(c["processed"])) / F(str(c["busy"]))


def needed_rate(c):
    return F(c["arriving"]) + F(c["backlog"], SETTINGS["backlog_catch_up_seconds"])


def formula(c):
    """The built-in rule: ceil(parallelism * needed capacity / current capacity), clamped."""
    target = F(SETTINGS["target_utilization_pct"], 100)
    current = capacity_per_subtask(c) * c["parallelism"]
    scale = (needed_rate(c) / target) / current
    n = math.ceil(scale * c["parallelism"])
    return max(SETTINGS["min_parallelism"], min(SETTINGS["max_parallelism"], n))


RULE = (
    "Rule (this is how the Flink autoscaler sizes a vertex):\n"
    "1. True capacity of one subtask = records processed per subtask per second / fraction of time busy.\n"
    "2. Current capacity = true capacity of one subtask x current parallelism.\n"
    "3. Rate to handle = records arriving per second + backlog records / backlog catch-up seconds.\n"
    "4. Capacity needed = rate to handle / target utilization.\n"
    "5. Scale factor = capacity needed / current capacity.\n"
    "6. New parallelism = current parallelism x scale factor, rounded UP to a whole number.\n"
    "7. Clamp the result to the minimum and maximum parallelism.\n"
    "If the result equals the current parallelism, nothing changes."
)


def measurements(c):
    m = {
        "vertex": "Aggregate",
        "current_parallelism": c["parallelism"],
        "records_arriving_per_sec": c["arriving"],
        "records_processed_per_subtask_per_sec": float(c["processed"]),
        "fraction_of_time_busy": float(c["busy"]),
        "backlog_records": c["backlog"],
    }
    if c["per_subtask_busy"]:
        m["fraction_of_time_busy"] = "average %s" % c["busy"]
        m["fraction_of_time_busy_per_subtask"] = c["per_subtask_busy"]
    return m


# ---------------------------------------------------------------- Jev

JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-1.14.0"
JEV_PRICE_IN = 0.042  # $ per million input tokens; output is free


def jev_question(c, computed):
    lo, hi = SETTINGS["min_parallelism"], SETTINGS["max_parallelism"]
    target = SETTINGS["target_utilization_pct"]
    crit = {}
    for n in range(lo, hi + 1):
        if not computed:
            crit[f"p{n}"] = f"New parallelism is {n} subtask{'s' if n > 1 else ''}"
            continue
        cap = capacity_per_subtask(c) * n
        util = float(needed_rate(c) / cap * 100)
        kind = "current setting" if n == c["parallelism"] else ("scale up" if n > c["parallelism"] else "scale down")
        verdict = f"above the {target}% target" if util > target + 1e-9 else f"at or below the {target}% target"
        crit[f"p{n}"] = (f"{n} subtask{'s' if n > 1 else ''} ({kind}): capacity {float(cap):.0f} rec/s, "
                         f"utilization {util:.1f}% including backlog catch-up; {verdict}")
    if computed:
        ins = (f"Choose the new parallelism for the vertex. Pick the option with the fewest subtasks that is at or "
               f"below the {target}% utilization target. If no option is at or below the target, pick the largest.")
    else:
        ins = "Choose the new parallelism for the vertex by applying this rule to the measurements.\n" + RULE
    return {"type": "choice", "instructions": ins, "criteria": crit}


def ask_jev(c, computed):
    key = os.environ.get("TYPESAFE_API_KEY")
    if not key:
        sys.exit("TYPESAFE_API_KEY is not set")
    body = json.dumps({
        "model": JEV_MODEL,
        "state": {"settings": SETTINGS, "measurements": measurements(c)},
        "questions": {"parallelism": jev_question(c, computed)},
    }).encode()
    for attempt in range(5):
        req = urllib.request.Request(JEV_URL, data=body, method="POST", headers={
            "Authorization": "Bearer " + key, "Content-Type": "application/json", "User-Agent": "model-eval/1.0"})
        t = time.time()
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                out = json.loads(r.read())
            a = out["answers"]["parallelism"]
            tok = (out.get("usage") or {}).get("input_tokens", 0)
            return dict(answer=int(a["choice"][1:]), confidence=a.get("confidence"),
                        probabilities=a.get("probabilities"), latency_ms=round((time.time() - t) * 1000),
                        input_tokens=tok, output_tokens=0, cost=tok * JEV_PRICE_IN / 1e6)
        except urllib.error.HTTPError as e:
            if e.code not in (429, 529) and e.code < 500:
                return dict(answer=None, error=f"HTTP {e.code}")
        except Exception as e:  # network error: retry
            err = str(e)
        time.sleep(1.5 * (attempt + 1))
    return dict(answer=None, error="retries exhausted")


# ---------------------------------------------------------------- Claude

CLAUDE = {  # name -> (model id, $/M input, $/M output)
    "haiku": ("claude-haiku-5-5", 0.10, 0.50),
    "sonnet": ("claude-sonnet-5-5", 2.00, 10.00),
    "opus": ("claude-opus-5-5", 4.00, 20.00),
    "fable": ("claude-fable-5-2", 8.00, 40.00),
    "sonnet-high": ("claude-sonnet-5-5", 2.00, 10.00),
}
EFFORT_OVERRIDE = {"sonnet-high": "high"}
# Answer only. Two earlier formats were dropped (ISSUES.md #12): "answer, then reason" let a model
# commit to a number before calculating, and asking for the working first is declined by the API's
# reasoning-extraction safeguard on some models. The models still reason internally before answering.
SCHEMA = {
    "type": "object",
    "properties": {"new_parallelism": {"type": "integer"}},
    "required": ["new_parallelism"],
    "additionalProperties": False,
}
_client = None


def claude_prompt(c):
    return (
        "You are deciding the parallelism of one vertex of a Flink streaming job.\n\n"
        + RULE
        + "\n\nSettings:\n" + json.dumps(SETTINGS, indent=2)
        + "\n\nMeasurements:\n" + json.dumps(measurements(c), indent=2)
        + "\n\nReturn the new parallelism for this vertex."
    )


def ask_claude(c, name, effort):
    import anthropic
    global _client
    if _client is None:
        _client = anthropic.Anthropic()  # credentials from the environment
    model, pin, pout = CLAUDE[name]
    cfg = {"effort": EFFORT_OVERRIDE.get(name, effort), "format": {"type": "json_schema", "schema": SCHEMA}}
    kwargs = dict(model=model, max_tokens=16000, messages=[{"role": "user", "content": claude_prompt(c)}])
    t = time.time()
    try:
        try:
            r = _client.messages.create(output_config=cfg, **kwargs)
        except TypeError:  # older SDK without the typed parameter
            r = _client.messages.create(extra_body={"output_config": cfg}, **kwargs)
    except anthropic.RateLimitError as e:
        return dict(answer=None, error="rate limited: %s" % e)
    except anthropic.APIStatusError as e:
        return dict(answer=None, error=f"HTTP {e.status_code}: {e.message}")
    except anthropic.APIConnectionError as e:
        return dict(answer=None, error="connection: %s" % e)
    lat = round((time.time() - t) * 1000)
    u = r.usage
    base = dict(latency_ms=lat, input_tokens=u.input_tokens, output_tokens=u.output_tokens,
                cost=(u.input_tokens * pin + u.output_tokens * pout) / 1e6)
    if r.stop_reason != "end_turn":
        cat = getattr(getattr(r, "stop_details", None), "category", None)
        return dict(answer=None, error="stop_reason %s%s" % (r.stop_reason, " (%s)" % cat if cat else ""), **base)
    text = "".join(b.text for b in r.content if b.type == "text")
    try:
        d = json.loads(text)
        return dict(answer=int(d["new_parallelism"]), **base)
    except Exception:
        return dict(answer=None, error="unparsable: " + text[:200], **base)


# ---------------------------------------------------------------- run / report

VARIANTS = ["jev-raw", "jev-computed", "haiku", "sonnet", "sonnet-high", "opus", "fable"]
LABEL = {"jev-raw": "Jev, raw inputs", "jev-computed": "Jev, computed options", "haiku": "Claude Haiku 5.5",
         "sonnet": "Claude Sonnet 5.5", "sonnet-high": "Claude Sonnet 5.5, high effort", "opus": "Claude Opus 5.5", "fable": "Claude Fable 5.2"}


def ask(variant, c, effort):
    if variant == "jev-raw":
        return ask_jev(c, False)
    if variant == "jev-computed":
        return ask_jev(c, True)
    return ask_claude(c, variant, effort)


def run(args):
    OUT.mkdir(parents=True, exist_ok=True)
    for variant in args.models.split(","):
        if variant not in VARIANTS:
            sys.exit(f"unknown model {variant}; choose from {VARIANTS}")
        jobs = [(c, i) for c in CASES for i in range(args.repeats)]
        rows = []
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(ask, variant, c, args.effort): (c, i) for c, i in jobs}
            for f in cf.as_completed(futs):
                c, i = futs[f]
                rows.append(dict(variant=variant, case=c["id"], repeat=i, expected=formula(c), **f.result()))
        rows.sort(key=lambda r: (r["case"], r["repeat"]))
        (OUT / f"{variant}.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        errs = sum(r["answer"] is None for r in rows)
        print(f"{variant}: {len(rows)} calls, {errs} errors")
        for r in rows:
            if r["answer"] is None:
                print("   ", r["case"], r.get("error"))
                break
    report(args)


def load():
    data = collections.defaultdict(lambda: collections.defaultdict(list))
    for v in VARIANTS:
        p = OUT / f"{v}.jsonl"
        if p.exists():
            for line in p.read_text().splitlines():
                r = json.loads(line)
                data[v][r["case"]].append(r)
    return data


def report(args):
    data = load()
    present = [v for v in VARIANTS if v in data]
    if not present:
        sys.exit("no results yet")
    L = ["# Model check against the built-in formula", "",
         "Settings: " + ", ".join(f"{k} = {v}" for k, v in SETTINGS.items()) + ".", "",
         "Each cell shows the answers given and how often, e.g. `5 ×9, 4 ×1`. "
         "**Bold** cells agree with the formula on every repeat.", "",
         "| Case | Current | Formula | " + " | ".join(LABEL[v] for v in present) + " |",
         "|---|---|---|" + "---|" * len(present)]
    for c in CASES:
        cells = []
        for v in present:
            rs = data[v].get(c["id"], [])
            cnt = collections.Counter("error" if r["answer"] is None else r["answer"] for r in rs)
            s = ", ".join(f"{a} ×{n}" for a, n in cnt.most_common())
            ok = c["scored"] and len(cnt) == 1 and formula(c) in cnt
            cells.append(f"**{s}**" if ok else s)
        exp = str(formula(c)) + ("" if c["scored"] else " (not scored)")
        L.append(f"| {c['title']} | {c['parallelism']} | {exp} | " + " | ".join(cells) + " |")
    scored_n = sum(1 for c in CASES if c["scored"])
    L += ["", f"## Scores over the {scored_n} scored cases", "",
          "| Model | Answers | Exact | Within 1 | Too low (leaves vertex above target) | Too high | Errors | "
          "Cases answered the same on every repeat | Median latency | Cost per decision |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    summary = {}
    for v in present:
        rs = [r for c in CASES if c["scored"] for r in data[v].get(c["id"], [])]
        ans = [r for r in rs if r["answer"] is not None]
        n = len(rs)
        exact = sum(r["answer"] == r["expected"] for r in ans)
        near = sum(abs(r["answer"] - r["expected"]) <= 1 for r in ans)
        low = sum(r["answer"] < r["expected"] for r in ans)
        high = sum(r["answer"] > r["expected"] for r in ans)
        stable = sum(len({r["answer"] for r in data[v].get(c["id"], [])}) == 1 for c in CASES if c["scored"])
        lats = sorted(r["latency_ms"] for r in rs if r.get("latency_ms") is not None)
        costs = [r["cost"] for r in rs if r.get("cost") is not None]
        med = lats[len(lats) // 2] if lats else float("nan")
        cost = sum(costs) / len(costs) if costs else float("nan")
        summary[v] = dict(n=n, exact=exact, near=near, low=low, high=high, errors=n - len(ans), stable=stable,
                          median_latency_ms=med, cost_per_decision=cost)
        L.append(f"| {LABEL[v]} | {n} | {exact} ({100 * exact / n:.0f}%) | {near} ({100 * near / n:.0f}%) | {low} | {high} | "
                 f"{n - len(ans)} | {stable} of {scored_n} | {med:,.0f} ms | ${cost:.6f} |")
    if len({c["title"] for c in CASES}) < len(CASES):  # grouped suite: exact answers per group
        groups = list(dict.fromkeys(c["title"] for c in CASES))
        L += ["", "## Exact answers per group", "", "| Model | " + " | ".join(groups) + " |", "|---|" + "---|" * len(groups)]
        for v in present:
            cells = []
            for g in groups:
                rs = [r for c in CASES if c["title"] == g for r in data[v].get(c["id"], [])]
                ok = sum(r["answer"] == r["expected"] for r in rs if r["answer"] is not None)
                cells.append(f"{ok} of {len(rs)}")
                summary[v].setdefault("by_group", {})[g] = [ok, len(rs)]
            L.append(f"| {LABEL[v]} | " + " | ".join(cells) + " |")
    (OUT / "summary.md").write_text("\n".join(L) + "\n")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
    print("\n".join(L))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=["fixed", "random"], default="fixed")
    ap.add_argument("--n", type=int, default=60, help="number of cases in the random suite")
    ap.add_argument("--seed", type=int, default=7)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--models", default="jev-raw,jev-computed")
    r.add_argument("--repeats", type=int, default=10)
    r.add_argument("--workers", type=int, default=4)
    r.add_argument("--effort", default="medium", help="Claude effort level (low|medium|high|xhigh|max)")
    sub.add_parser("report")
    a = ap.parse_args()
    if a.suite == "random":
        CASES = random_cases(a.n, a.seed)
        OUT = OUT.parent / "model_eval_random"
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "cases.json").write_text(json.dumps(
            [dict(c, expected=formula(c)) for c in CASES], indent=2))
    run(a) if a.cmd == "run" else report(a)
