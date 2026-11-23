"""Runs next to the harness in the driver pod and records evidence when a job's Measure vertex hangs.

Seen on the cluster at scale 4 (ISSUES.md #103): the job stays RUNNING, the Aggregate vertex keeps
emitting, but the Measure subtasks take no more input and no latency summary reaches Kafka. Every
10 s this reads, for each test FlinkDeployment, the record counters of Aggregate and Measure from the
JobManager. When Measure took nothing for 20 s while Aggregate emitted, it saves thread dumps of the
job's TaskManagers, the checkpoint statistics and the pods' logs under /results/hang/. It changes
nothing in the cluster.

    python3 hangwatch.py            (CLUSTER_CONF names the cluster settings, as for bench_cluster.py)
"""
import json
import os
import subprocess
import time
import urllib.request

CONF = json.load(open(os.environ.get("CLUSTER_CONF", "/etc/asbench/cluster.json")))
NS = CONF["namespace"]
OUT = os.environ.get("HANG_OUT", "/results/hang")
POLL = 10


def log(*a):
    print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), *a, flush=True)


def kubectl(*args):
    return subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=60).stdout


def get(dep, path, timeout=6):
    with urllib.request.urlopen(f"http://{dep}-rest.{NS}.svc:8081{path}", timeout=timeout) as r:
        return json.load(r)


def counter(dep, jid, vid, name):
    rows = get(dep, f"/jobs/{jid}/vertices/{vid}/subtasks/metrics?get={name}")
    return rows[0]["sum"] if rows else None


def capture(dep, jid, vertices, why):
    d = os.path.join(OUT, f"{dep}-{int(time.time())}")
    os.makedirs(d, exist_ok=True)
    log("CAPTURE", dep, why, "->", d)

    def save(name, fn):
        try:
            data = fn()
            with open(os.path.join(d, name), "w") as f:
                f.write(data if isinstance(data, str) else json.dumps(data))
        except Exception as e:  # keep going: every file is useful on its own
            log("  could not save", name, repr(e)[:200])

    save("why.txt", lambda: why)
    save("job.json", lambda: get(dep, f"/jobs/{jid}"))
    save("checkpoints.json", lambda: get(dep, f"/jobs/{jid}/checkpoints"))
    for name, vid in vertices.items():
        save(f"vertex-{name}.json", lambda vid=vid: get(dep, f"/jobs/{jid}/vertices/{vid}"))
        save(f"backpressure-{name}.json", lambda vid=vid: get(dep, f"/jobs/{jid}/vertices/{vid}/backpressure"))
    try:
        tms = [t["id"] for t in get(dep, "/taskmanagers")["taskmanagers"]]
    except Exception as e:
        tms = []
        log("  no TaskManager list", repr(e)[:200])
    for round_ in (1, 2):
        for tm in tms:
            save(f"threads-{round_}-{tm}.json", lambda tm=tm: get(dep, f"/taskmanagers/{tm}/thread-dump", timeout=20))
        if round_ == 1:
            time.sleep(15)
    for pod in kubectl("get", "pods", "-l", f"app={dep}", "-o", "name").split():
        save("log-" + pod.split("/")[-1] + ".txt",
             lambda pod=pod: kubectl("logs", pod, "-c", "flink-main-container", "--tail=4000"))


def main():
    os.makedirs(OUT, exist_ok=True)
    state = {}  # (deployment, job id) -> [measure in, aggregate out, seconds without Measure input, captures]
    log("watching", NS)
    while True:
        try:
            deps = kubectl("get", "flinkdeployment", "-l", "asbench=true", "-o",
                           "jsonpath={.items[*].metadata.name}").split()
        except Exception as e:
            log("kubectl failed", repr(e)[:200])
            deps = []
        for dep in deps:
            try:
                jobs = get(dep, "/jobs/overview")["jobs"]
                if not jobs or jobs[0]["state"] != "RUNNING":
                    continue
                jid = jobs[0]["jid"]
                vs = {("measure" if "Measure" in v["name"] else "aggregate" if "Aggregate" in v["name"] else "source"): v["id"]
                      for v in get(dep, f"/jobs/{jid}")["vertices"]}
                if "measure" not in vs or "aggregate" not in vs:
                    continue
                m_in = counter(dep, jid, vs["measure"], "numRecordsIn")
                a_out = counter(dep, jid, vs["aggregate"], "numRecordsOut")
                if m_in is None or a_out is None:
                    continue
                st = state.setdefault((dep, jid), [m_in, a_out, 0, 0])
                # A rescale restarts the counters: a drop means new tasks, so start over.
                if m_in < st[0] or a_out < st[1]:
                    st[0], st[1], st[2] = m_in, a_out, 0
                    continue
                if m_in == st[0] and a_out - st[1] > 50:
                    st[2] += POLL
                elif m_in > st[0]:
                    st[2] = 0
                st[0], st[1] = m_in, a_out
                if st[2] >= 20 and st[3] < 2:
                    st[3] += 1
                    st[2] = 0
                    capture(dep, jid, vs, f"Measure took no input for 20 s while Aggregate emitted (in={m_in}, aggregate out={a_out})")
            except Exception as e:
                # A job that is starting or restarting does not answer: not an error.
                if "timed out" not in repr(e) and "refused" not in repr(e) and "Name or service" not in repr(e):
                    log(dep, repr(e)[:200])
        time.sleep(POLL)


if __name__ == "__main__":
    main()
