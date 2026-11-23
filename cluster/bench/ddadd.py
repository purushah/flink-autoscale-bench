"""Adds the monitoring service's numbers of one run to <campaign dir>/datadog.json.
   python3 ddadd.py <campaign dir> <scenario/rep> <from> <to> job=over,measured,total,free ...
Columns: seconds over the limit, seconds measured, slots registered (average), slots free (average)."""
import json, os, sys
camp, key, start, end = sys.argv[1:5]
path = os.path.join(camp, "datadog.json")
d = json.load(open(path)) if os.path.exists(path) else {
    "source": "monitoring service, scalar queries per run window (t0 to t0 + duration of meta.json), the four queries of results-cluster/flapping/datadog.json",
    "columns": ["seconds_over_limit", "seconds_measured", "slots_total_avg", "slots_available_avg"], "windows": {}, "runs": {}}
d["windows"][key] = [start, end]
d["runs"][key] = {}
for a in sys.argv[5:]:
    job, _, vals = a.partition("=")
    d["runs"][key][job] = [float(x) for x in vals.split(",")]
    assert len(d["runs"][key][job]) == 4, a
json.dump(d, open(path, "w"), indent=1)
print(key, {j: v[0] for j, v in d["runs"][key].items()}, "runs in file:", len(d["runs"]))
