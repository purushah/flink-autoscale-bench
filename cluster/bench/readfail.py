"""Check, without any outcome: did the plugin fail to read the backlog from the brokers between the two
load changes of a cluster run? Counts the operator's lines "Could not read the backlog from the broker"
in that window (plan entries of 21 Nov 13:10 and 22 Nov 09:06 PST) and says what the log covers.
   python3 readfail.py <run dir>..."""
import calendar, json, os, re, sys, time
import oplog
STAMP = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d\d\d")
for d in sys.argv[1:]:
    m = json.load(open(os.path.join(d, "meta.json")))
    a, b = m["t0_epoch"] + m["events"][0], m["t0_epoch"] + m["events"][1]
    n = inside = 0
    first = last = None
    for line in oplog.lines(d):
        s = STAMP.search(line)
        if not s:
            continue
        t = calendar.timegm(time.strptime(s.group(1), "%Y-%m-%d %H:%M:%S"))
        first = t if first is None else min(first, t)
        last = t if last is None else max(last, t)
        if "Could not read the backlog from the broker" in line:
            n += 1
            inside += a <= t <= b
    cov = "no stamped line" if first is None else f"log from {first - a:+.0f} s to {last - b:+.0f} s relative to the rise and the fall"
    print(f"{d}: 'Could not read the backlog' lines: {n} in the run's log, {inside} between the two load changes; {cov}")
