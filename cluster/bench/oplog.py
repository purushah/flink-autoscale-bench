"""The operator's log of a cluster run, from both copies the harness keeps.
   import oplog; for line in oplog.lines(run_dir): ...
   python3 oplog.py <run dir>...     prints what each copy covers
<run>/operator.log is fetched once when the run ends. After a rotation of the container's log it starts
late (ISSUES.md #109, #126). <run>/logs/operator.follow.log is written while the run lasts by
`kubectl logs -f --timestamps`; its lines carry that command's time stamp in front and repeat where the
follower was started again. lines() gives the union of the two without colour codes and without the
front time stamp, each line once, ordered by the operator's own time stamp. A line without a time stamp
(the rest of a multi-line message) keeps its place after the line before it; an identical line met a
second time is dropped."""
import os, re, sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
FRONT = re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+Z ")
STAMP = re.compile(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d")


def read(path, front):
    out, key = [], ""
    for line in open(path, errors="replace"):
        line = ANSI.sub("", line)
        if front:
            line = FRONT.sub("", line)
        m = STAMP.match(line)
        if m:
            key = m.group(0)
        out.append((key, line))
    return out


def lines(d):
    out, seen = [], set()
    for path, front in ((os.path.join(d, "logs", "operator.follow.log"), True), (os.path.join(d, "operator.log"), False)):
        if not os.path.exists(path):
            continue
        for key, line in read(path, front):
            if line in seen:
                continue
            seen.add(line)
            out.append((key, len(out), line))
    out.sort(key=lambda x: (x[0], x[1]))
    return [x[2] for x in out]


if __name__ == "__main__":
    for d in sys.argv[1:]:
        parts = []
        for name, front in (("logs/operator.follow.log", True), ("operator.log", False)):
            path = os.path.join(d, name)
            if not os.path.exists(path):
                parts.append(f"{name}: none")
                continue
            keys = [k for k, _ in read(path, front) if k]
            parts.append(f"{name}: {len(keys)} stamped lines, {keys[0] if keys else '-'} to {keys[-1] if keys else '-'}")
        both = lines(d)
        print(f"{d}: " + "; ".join(parts) + f"; union {len(both)} lines, {sum('SCALINGREPORT' in x for x in both)} scaling reports")
