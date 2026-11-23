"""On-off runs: in which phase the Aggregate changes after the first cycle came into effect. Per job over
the runs given: scale-downs, how many of them fell into a high phase (and how many seconds after the
load had come back), scale-ups, and how many of them fell into a low phase. The changes are those of
flapsum.py.
   python3 flapwhen.py <run dir>..."""
import collections, json, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
count = collections.defaultdict(collections.Counter)
after_jump = collections.defaultdict(list)
order = []
for d in sys.argv[1:]:
    ev = json.load(open(os.path.join(d, "meta.json")))["events"]
    out = subprocess.run([sys.executable, os.path.join(HERE, "flapsum.py"), d], capture_output=True, text=True).stdout
    for line in out.splitlines():
        m = re.match(r"\s{6}(\S+)\s*((?:\d+s \d+->\d+(?:, )?)*)\s*$", line)
        if not m:
            continue
        job = m.group(1)
        if job not in order:
            order.append(job)
        for t, a, b in re.findall(r"(\d+)s (\d+)->(\d+)", m.group(2)):
            t, a, b = float(t), int(a), int(b)
            passed = [e for e in ev if e <= t]
            high = len(passed) % 2 == 1  # the load rises at the 1st, 3rd, ... event
            c = count[job]
            if b < a:
                c["downs"] += 1
                if high:
                    c["downs_high"] += 1
                    after_jump[job].append(round(t - max(passed)))
            else:
                c["ups"] += 1
                c["ups_low"] += not high
print(f"{len(sys.argv) - 1} runs")
for job in order:
    c = count[job]
    print(f"   {job:<14} scale-downs {c['downs']:>2}, in a high phase {c['downs_high']:>2} "
          f"(s after the jump: {', '.join(map(str, sorted(after_jump[job]))) or '-'}); "
          f"scale-ups {c['ups']:>2}, in a low phase {c['ups_low']:>2}")
