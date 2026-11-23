"""Laptop runs measured next to another workload on the host (plan amendment of 2026-11-21 18:39 PST).
   python3 bench/disturbed.py <run dir>...
A run is disturbed from outside if, in any sample of testbed/interference.log between its start and its
end, the containers other than the testbed's node together used more than 100% of one processor, or if
a pod of the testbed that is not a job's pod restarted during the run (testbed/restarts.log, and
testbed/terminations.txt for the restarts before that log began). The run's
window is from `started_utc` in its meta.json to the time that file was written; no outcome is read."""
import calendar, json, os, re, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE = "jev-bench-control-plane"


def local(stamp):
    return time.mktime(time.strptime(stamp, "%Y-%m-%d %H:%M:%S"))


def samples():
    out = []
    for line in open(os.path.join(ROOT, "testbed", "interference.log")):
        m = re.match(r"(\S+ \S+) load\s+(\S+) \S+ \S+\s+\| (.*)", line)
        if not m:
            continue
        other = sum(float(p.split("=", 1)[1].split("%")[0]) for p in m.group(3).split() if not p.startswith(NODE + "="))
        out.append((local(m.group(1)), other, float(m.group(2))))
    return out


def restarts():
    out, last = [], None
    path = os.path.join(ROOT, "testbed", "restarts.log")
    if not os.path.exists(path):
        return out
    for line in open(path):
        m = re.match(r"(\S+ \S+) (.*)", line)
        if not m or "=" not in m.group(2):
            continue
        now = dict(p.split("=", 1) for p in m.group(2).split())
        if last:
            for pod, n in now.items():
                if pod in last[1] and n != last[1][pod]:
                    out.append((last[0], local(m.group(1)), pod))
        last = (local(m.group(1)), now)
    known = os.path.join(ROOT, "testbed", "terminations.txt")  # restarts from before restarts.log began
    if os.path.exists(known):
        for line in open(known):
            m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)Z (\S+)", line)
            if m:
                t = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
                out.append((t, t, m.group(2)))
    return out


def main():
    ss, rs = samples(), restarts()
    for d in sys.argv[1:]:
        meta = os.path.join(d, "meta.json")
        if not os.path.exists(meta):
            print(f"{d}: no meta.json (the harness rejected the run or it failed)")
            continue
        m = json.load(open(meta))
        utc = lambda s: calendar.timegm(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S"))
        a, b = utc(m["started_utc"]), os.path.getmtime(meta)  # meta.json is written when the run ends
        inside = [s for s in ss if a <= s[0] <= b]
        high = [s for s in inside if s[1] > 100]
        hit = [r for r in rs if r[1] >= a and r[0] <= b]
        f = lambda t: time.strftime("%H:%M:%S", time.localtime(t))
        verdict = "DISTURBED" if high or hit else ("no sample in its window" if not inside else "clean")
        print(f"{d}: {f(a)} to {f(b)} local, {len(inside)} samples, {len(high)} with other containers above 100% "
              f"(largest {max((s[1] for s in inside), default=0):.0f}%), largest load average {max((s[2] for s in inside), default=0):.0f}, "
              f"restarts of testbed pods {[r[2] for r in hit]}: {verdict}")


if __name__ == "__main__":
    main()
