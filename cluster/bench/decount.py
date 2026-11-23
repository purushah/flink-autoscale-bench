"""Model against policy in cluster runs, by the definition the plan fixed for the laptop campaigns
(paper/outline.md, 2026-11-18 17:47 PST): decisions of the measured phase in which the model was asked
about a vertex (more than one option), the model's most likely option against the policy's choice.
Per job over the runs given: asked, same, above, below, and how many of the differing choices were applied.
   python3 decount.py <run dir>..."""
import calendar, collections, json, os, re, sys, time


def epoch(ts):
    whole, _, frac = ts.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(whole, "%Y-%m-%dT%H:%M:%S")) + (float("0." + frac) if frac else 0.0)


def option(text):
    m = re.fullmatch(r"p(\d+)", str(text or ""))
    return int(m.group(1)) if m else None


count = collections.defaultdict(collections.Counter)
for d in sys.argv[1:]:
    meta = json.load(open(os.path.join(d, "meta.json")))
    t0, dur = meta["t0_epoch"], meta["duration"]
    seen = set()
    for line in open(os.path.join(d, "decisions.jsonl")):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        j = re.search(r"name='bench-([^']+)'", o.get("job", ""))
        if not j or o.get("event") != "decision" or o.get("status") != "DECIDED" or not o.get("model") or o["model"] == "rule":
            continue
        key = (j.group(1), o["ts"])
        if key in seen:  # a rebuilt log can hold a decision twice
            continue
        seen.add(key)
        if not 0 <= epoch(o["ts"]) - t0 <= dur:
            continue
        for v in (o.get("vertices") or {}).values():
            choice, rule = option(v.get("choice")), v.get("rule_choice")
            if choice is None or rule is None or rule < 0:
                continue
            c = count[j.group(1)]
            c["asked"] += 1
            c["same"] += choice == rule
            c["above"] += choice > rule
            c["below"] += choice < rule
            if choice != rule and v.get("target") == choice:
                c["applied"] += 1
print(f"{len(sys.argv) - 1} runs")
for job in sorted(count):
    c = count[job]
    print(f"   {job:<16} asked {c['asked']:>4}  same {c['same']:>4}  above {c['above']:>3}  below {c['below']:>3}  differing and applied {c['applied']:>3}")
