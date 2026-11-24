#!/usr/bin/env python3
"""Runs one benchmark scenario against several identical Flink jobs at the same time. The jobs
differ only in who decides their parallelism:

  bench-builtin      the operator's built-in autoscaler (builtinNN: utilization target 0.NN)
  bench-rule         the plugin's policy applied in code, without a model (control)
  bench-jev          the plugin with the Jev decision model
  bench-llm          the plugin with a Claude model (llm-sonnet, llm-opus, llm-fable: other tiers)

All jobs read identical records (same sequence, keys, costs and timing) from their own Kafka
topic. While the load runs, the script samples Flink, Kubernetes and Kafka every few seconds and
collects the per-second latency summaries the job writes to its output topic.

Usage:
  python3 bench/bench.py run --scenario spike --rep 1
  python3 bench/bench.py all --reps 3 --jobs builtin,rule,jev,llm
  python3 bench/bench.py all --reps 3 --jobs builtin,rule,jev,llm --fetch-bytes 4096
"""

import argparse
import csv
import json
import math
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scenarios  # noqa: E402
from pacing import Pacer, parse_burst  # noqa: E402
from startcheck import jobs_ready, keep_warming, start_state  # noqa: E402
from scenarios import HOT_KEYS, MEASURE_COST, NUM_KEYS, SCENARIOS  # noqa: E402

# The broker advertises localhost:9094, and that port is forwarded to the cluster on IPv4 only. When
# the host resolves localhost to ::1 first (it does once an interface with IPv6 is up, for example a
# VPN), kafka-python 2.3.0 takes the refused IPv6 connection for an open one and fails with
# "Invalid file object: None" instead of trying 127.0.0.1. The harness therefore resolves names to
# IPv4 only (ISSUES.md #35).
_getaddrinfo = socket.getaddrinfo


def _ipv4_only(host, port, family=0, *args, **kwargs):
    return _getaddrinfo(host, port, socket.AF_INET, *args, **kwargs)


socket.getaddrinfo = _ipv4_only

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CTX = "kind-jev-bench"
NS = "flink"
BROKERS = "localhost:9094"
PROXY_PORT = 8001
PROXY = f"http://127.0.0.1:{PROXY_PORT}"
PARTITIONS = 8
JOBS = ("builtin", "jev")
VERTICES = ("source", "aggregate", "measure")
SAMPLE_INTERVAL = 5.0
KEY_GROUPS = 120  # pipeline.max-parallelism of every job
SETTLE_S = 105  # the measured phase starts when no job has restarted or rescaled for this long:
# stabilization interval (30 s) + metric window (60 s) + one reconcile interval (15 s)
METRIC_REFRESH_S = 1.0  # wait between asking Flink to refresh its metrics and reading them
BURST = None  # --burst PERIOD_S:DUTY: the load leaves in bursts at the same average rate (Issue #141)
START_WAIT = False  # --start-wait: the warm-up also waits until every job runs at the scenario's size (Issue #146)
DRAIN_MAX = 240  # longest extra warm-up (s) spent waiting for every job to work off its backlog
MIN_FREE_GB = 8  # below this the Rancher Desktop VM disk can go read-only mid-run (Issue #13)
FETCH_BYTES = None  # max.partition.fetch.bytes for the jobs' Kafka consumers; None = Kafka default
SOURCE_PARALLELISM = None  # fixed source parallelism for every job; None = autoscaled
RECORD_BYTES = 0  # pad every record value to this many bytes; 0 = no padding (about 40 bytes)
JITTER = 0  # the scenario's changes are delayed by a random time of up to this many seconds per run
FAULT = None  # (kind, start, length): model calls of all jobs fail ("error") or block ("hang") from
#               `start` seconds after the scenario's first change, for `length` seconds
# The JobManager's JVM is sized as for a 1024m process (heap 448m, metaspace 256m). Its container
# gets more than that: with rescales the JVM's real footprint reached 1 GiB after about ten minutes
# and the kernel killed the JobManager in the middle of a run (Issue #61).
JM_HEAP = "448m"
JM_CONTAINER_MEMORY = "1280m"
FAULT_FILE = "/tmp/arbiter-fault"  # the operator pod's ARBITER_FAULT_FILE

# Autoscaler settings shared by both jobs. The windows are much shorter than the production
# defaults so that a scenario fits in a few minutes.
COMMON_AUTOSCALER = {
    "job.autoscaler.enabled": "true",
    "job.autoscaler.metrics.window": "1m",
    "job.autoscaler.stabilization.interval": "30s",
    "job.autoscaler.utilization.target": "0.7",
    "job.autoscaler.restart.time": "30s",
    "job.autoscaler.catch-up.duration": "1m",
    "job.autoscaler.scale-down.interval": "2m",
    "job.autoscaler.vertex.min-parallelism": "1",
    "job.autoscaler.vertex.max-parallelism": "8",
    "job.autoscaler.scaling.event.interval": "10s",
}

JEV_AUTOSCALER = {
    "job.autoscaler.metrics.custom-evaluators": "jev",
    "job.autoscaler.metrics.custom-evaluator.jev.class": "dev.jevscaler.JevEvaluatorPlugin",
    "job.autoscaler.scaling.custom-executors": "jev",
    "job.autoscaler.scaling.custom-executor.jev.class": "dev.jevscaler.JevExecutorPlugin",
    "job.autoscaler.jev.model": "jev-1.14.0",
    "job.autoscaler.jev.max-parallelism": "8",
    "job.autoscaler.jev.cooldown.scale-up": "45s",
    "job.autoscaler.jev.cooldown.scale-down": "150s",
}

# The same plugin with a Claude model deciding instead of Jev (job "llm"). Limits, cooldowns and
# fallback are identical; only the model and the way it is asked differ.
LLM_AUTOSCALER = {
    **{k: v for k, v in JEV_AUTOSCALER.items() if k != "job.autoscaler.jev.model"},
    "job.autoscaler.jev.provider": "anthropic",
    "job.autoscaler.jev.llm.model": "claude-haiku-5-5",
}


# The built-in job loads the evaluator plugin in observe-only mode: it logs what the autoscaler
# believes each cycle (demand, backlog, capacity) and never changes a metric or a decision.
OBSERVE_AUTOSCALER = {
    "job.autoscaler.metrics.custom-evaluators": "jev",
    "job.autoscaler.metrics.custom-evaluator.jev.class": "dev.jevscaler.JevEvaluatorPlugin",
    "job.autoscaler.jev.provider": "observe",
}

# The plugin's own policy applied in code, without any model call (job "rule"). Same limits and
# cooldowns as Jev and Claude, so it separates what the model adds from what the code around it does.
RULE_AUTOSCALER = {
    **{k: v for k, v in JEV_AUTOSCALER.items() if k != "job.autoscaler.jev.model"},
    "job.autoscaler.jev.provider": "rule",
}

LLM_MODELS = {"haiku": "claude-haiku-5-5", "sonnet": "claude-sonnet-5-5", "opus": "claude-opus-5-5",
              "fable": "claude-fable-5-2"}
# Vertex ids follow from the operator uids in BenchJob, so they are the same in every run.
SOURCE_VERTEX = "e5a72f353fc1e6bbf3bd96a41384998c"

# A job is named by a spec: a controller followed by options, joined with "+".
#   builtin | rule | jev | llm        who decides the parallelism
#   +maxNN +minNN +tNN +rtN           built-in utilization.max / .min / .target (percent), restart.time (s)
#   +sonnet +opus +fable              Claude tier for llm (default haiku)
#   +fN                               max.partition.fetch.bytes of this job's Kafka consumer
#   +spN                              source parallelism fixed at N and excluded from autoscaling
#   +aggN                             Aggregate parallelism at start (fixed when the autoscaler is off)
#   +off                              autoscaler disabled
#   +noplugin                         built-in without the observe-only plugin
#   +lagfix                           backlog and demand of the source from broker end offsets and the
#                                     reader position (plugin class BrokerLag) instead of pendingRecords
#   +rdN                              the Aggregate operator waits N seconds when it (re)starts, standing in
#                                     for the restore of a large state
#   +rrN                              the same delay on restores only, not on the first start: a job
#                                     with it does not build a backlog in the warm-up (Issue #146)
#   +advc +advl +advd                 marks the job as advised by an advisor outside the sizing loop: rules
#                                     in code, an LLM or a decision model. Changes nothing in the job itself.
#   +a ... +h                         a tag, to run several identical jobs side by side
SPEC = re.compile(r"(builtin|rule|jev|llm)((\+(max\d\d|min\d\d|t\d\d|rt\d+|sonnet|opus|fable|f\d+|sp\d+|agg\d+|off|noplugin|lagfix|rd\d+|rr\d+|advc|advl|advd|[a-h]))*)")


def options(job):
    """The options of a job spec as a dict, e.g. 'builtin+max70+sp2' -> {'max': 70, 'sp': 2}."""
    out = {}
    for o in job.split("+")[1:]:
        m = re.fullmatch(r"([a-z]+?)(\d+)", o)
        out[m.group(1) if m else o] = int(m.group(2)) if m else True
    return out


def safe(job):
    """The job spec as it appears in Kubernetes and Kafka names."""
    return job.replace("+", "-")


def autoscaler_conf(job, jev_overrides=None):
    """The autoscaler settings of one job, on top of the settings all jobs share."""
    base, o = job.split("+")[0], options(job)
    conf = dict(COMMON_AUTOSCALER)
    if base == "builtin":
        if "noplugin" not in o:
            conf.update(OBSERVE_AUTOSCALER)
    elif base == "jev":
        conf.update(JEV_AUTOSCALER)
        conf.update(jev_overrides or {})
    elif base == "rule":
        conf.update(RULE_AUTOSCALER)
    elif base == "llm":
        conf.update(LLM_AUTOSCALER)
        for tier in ("sonnet", "opus", "fable"):
            if tier in o:
                conf["job.autoscaler.jev.llm.model"] = LLM_MODELS[tier]
    for key, name in (("max", "utilization.max"), ("min", "utilization.min"), ("t", "utilization.target")):
        if key in o:
            conf[f"job.autoscaler.{name}"] = f"0.{o[key]:02d}"
    if "rt" in o:
        conf["job.autoscaler.restart.time"] = f"{o['rt']}s"
    if "lagfix" in o:
        conf["job.autoscaler.jev.lag.source"] = "broker"
    if "off" in o:
        conf["job.autoscaler.enabled"] = "false"
    if "sp" in o or SOURCE_PARALLELISM:
        conf["job.autoscaler.vertex.exclude.ids"] = SOURCE_VERTEX
    return conf


def duration_s(text):
    """A Flink duration such as 30s, 1m or 5 min, in seconds."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-z]*)\s*", str(text).lower())
    unit = m.group(2)
    return float(m.group(1)) * (0.001 if unit == "ms" else 60 if unit.startswith("m") else 3600 if unit.startswith("h") else 1)


def initial_parallelism(scenario, conf):
    """(Aggregate, Measure) parallelism at which a job starts: what the sizing formula of the built-in
    rule gives for the scenario's initial load with the job's autoscaler settings, aligned to the key
    groups. Jobs that start anywhere else rescale during the warm-up, and the built-in rule in
    particular scales Measure down a few minutes after the start, which leaves it without a full
    metric window when the scenario begins (Issue #31)."""
    target = float(conf["job.autoscaler.utilization.target"])
    restart = duration_s(conf["job.autoscaler.restart.time"])
    catch_up = duration_s(conf["job.autoscaler.catch-up.duration"])
    limit = int(conf["job.autoscaler.vertex.max-parallelism"])
    need = scenario.rate(0) * (1 / target + restart / catch_up)  # records/s of capacity
    agg = math.ceil(need * scenario.agg_cost(0) / 1e6 - 1e-9)
    agg = next((p for p in range(max(1, agg), limit + 1) if KEY_GROUPS % p == 0), limit)
    measure = min(limit, max(1, math.ceil(need * scenarios.MEASURE_COST / 1e6 - 1e-9)))
    return agg, measure


def start_sizes(job, scenario, jev_overrides=None):
    """(Aggregate, Measure) parallelism a job is started with: the scenario's size by the rule's formula,
    unless the job spec fixes it (+aggN, +off). The start check compares the running job with this."""
    o = options(job)
    agg, measure = initial_parallelism(scenario, autoscaler_conf(job, jev_overrides))
    return o.get("agg", agg), 1 if "off" in o else measure


def job_args(job, run_id, scenario=None):
    """Program arguments of one job."""
    o = options(job)
    args = ["--in-topic", f"in-{safe(job)}-{run_id}", "--out-topic", f"out-{safe(job)}-{run_id}",
            "--group", f"bench-{safe(job)}-{run_id}"]
    fetch = o.get("f", FETCH_BYTES)
    if fetch:
        args += ["--max-partition-fetch-bytes", str(fetch)]
    source = o.get("sp", SOURCE_PARALLELISM)
    if source:
        args += ["--source-parallelism", str(source)]
    agg, measure = start_sizes(job, scenario) if scenario else (o.get("agg"), 1 if "off" in o else None)
    if agg:
        args += ["--agg-parallelism", str(agg)]
    if measure:
        args += ["--measure-parallelism", str(measure)]
    if "rd" in o:
        args += ["--restore-delay-ms", str(o["rd"] * 1000)]
    if "rr" in o:
        args += ["--restore-delay-ms", str(o["rr"] * 1000), "--restore-delay-first-start", "false"]
    return args


def sh(*args, check=True, capture=True):
    r = subprocess.run(args, capture_output=capture, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {r.stderr.strip()[:500]}")
    return r.stdout if capture else ""


def kubectl(*args, check=True):
    return sh("kubectl", "--context", CTX, *args, check=check)


def http_json(path, timeout=4):
    with urllib.request.urlopen(PROXY + path, timeout=timeout) as r:
        return json.load(r)


def flink(job, path):
    return http_json(f"/api/v1/namespaces/{NS}/services/bench-{safe(job)}-rest:8081/proxy{path}")


def deployment(job, run_id, jev_overrides=None, scenario=None):
    conf = {
        "taskmanager.numberOfTaskSlots": "4",
        "jobmanager.scheduler": "adaptive",
        "jobmanager.adaptive-scheduler.resource-stabilization-timeout": "5s",
        "jobmanager.adaptive-scheduler.scaling-interval.min": "5s",
        "resourcemanager.taskmanager-timeout": "15s",
        "state.backend.type": "hashmap",
        "execution.checkpointing.interval": "10s",
        "state.checkpoints.dir": f"file:///flink-data/{run_id}/{job}/checkpoints",
        "state.savepoints.dir": f"file:///flink-data/{run_id}/{job}/savepoints",
        "high-availability.type": "kubernetes",
        "high-availability.storageDir": f"file:///flink-data/{run_id}/{job}/ha",
        "pipeline.max-parallelism": str(KEY_GROUPS),
        "jobmanager.memory.heap.size": JM_HEAP,
        # Flink answers a metric query from the snapshot the previous query asked for and starts a
        # new one if the last is older than this; the sampler therefore asks twice (METRIC_REFRESH_S).
        "metrics.fetcher.update-interval": "1000",
        # Keep only about one second of data in network buffers, so a backlog shows up as Kafka
        # lag (which the autoscaler can see) instead of hiding inside the job.
        "taskmanager.network.memory.buffer-debloat.enabled": "true",
        "restart-strategy.type": "fixed-delay",
        "restart-strategy.fixed-delay.attempts": "1000",
        "restart-strategy.fixed-delay.delay": "2s",
    }
    conf.update(autoscaler_conf(job, jev_overrides))
    volume = {"name": "flink-data", "persistentVolumeClaim": {"claimName": "flink-data"}}
    mount = {"name": "flink-data", "mountPath": "/flink-data"}
    return {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {"name": f"bench-{safe(job)}", "namespace": NS},
        "spec": {
            "image": "bench-job:dev",
            "imagePullPolicy": "IfNotPresent",
            "flinkVersion": "v1_20",
            "serviceAccount": "flink",
            "mode": "native",
            "flinkConfiguration": conf,
            "podTemplate": {
                "spec": {
                    "volumes": [volume],
                    "containers": [{"name": "flink-main-container", "volumeMounts": [mount]}],
                }
            },
            "jobManager": {"resource": {"memory": JM_CONTAINER_MEMORY, "cpu": 0.5}},
            "taskManager": {"resource": {"memory": "1024m", "cpu": 0.5}},
            "job": {
                "jarURI": "local:///opt/flink/usrlib/bench-job.jar",
                "entryClass": "dev.jevscaler.job.BenchJob",
                "args": job_args(job, run_id, scenario),
                "parallelism": 2,
                "upgradeMode": "last-state",
                "state": "running",
            },
        },
    }


def delete_deployments():
    kubectl("-n", NS, "delete", "flinkdeployment", "--all", "--wait=true", "--timeout=180s", check=False)
    deadline = time.time() + 180
    while time.time() < deadline:
        pods = kubectl("-n", NS, "get", "pods", "-o", "name", check=False).split()
        if not pods:
            return
        time.sleep(3)
    print("WARNING: pods still present after delete", flush=True)


def wait_running(timeout=420):
    deadline = time.time() + timeout
    job_ids = {}
    while time.time() < deadline:
        ok = True
        for job in JOBS:
            try:
                d = http_json(f"/apis/flink.apache.org/v1beta1/namespaces/{NS}/flinkdeployments/bench-{safe(job)}")
                jid = d.get("status", {}).get("jobStatus", {}).get("jobId")
                detail = flink(job, f"/jobs/{jid}") if jid else None
                if detail and detail["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in detail["vertices"]):
                    job_ids[job] = jid
                else:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return job_ids
        time.sleep(4)
    raise RuntimeError("jobs did not reach RUNNING in time")


class Clock:
    """Start of the measured phase (epoch seconds). Unknown during the warm-up, because the warm-up
    lasts until every job has worked off its backlog."""

    t0 = None


def shift_times(path, t0, digits):
    """Rewrites the first column of a CSV from epoch seconds to seconds since the measured phase began."""
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    col = rows[0].index("t")
    for r in rows[1:]:
        r[col] = round(float(r[col]) - t0, digits)
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)


def set_fault(kind):
    """Switches the injected model fault of the operator on (kind) or off (None)."""
    cmd = f"echo {kind} > {FAULT_FILE}" if kind else f"rm -f {FAULT_FILE}"
    kubectl("-n", "flink-operator", "exec", "deployment/flink-kubernetes-operator", "--", "sh", "-c", cmd, check=False)


class LoadGenerator(threading.Thread):
    """Sends the scenario's records to every job's input topic with identical content and timing."""

    def __init__(self, scenario, run_id, clock, out_path, rep=1, shift=0.0):
        super().__init__(daemon=True)
        self.s, self.run_id, self.clock, self.out_path = scenario, run_id, clock, out_path
        self.rep, self.shift = rep, shift
        self.sent = 0
        self.error = None

    def run(self):
        try:
            self._run()
        except Exception as e:  # surfaced by the main thread
            self.error = e

    def _run(self):
        producer = KafkaProducer(bootstrap_servers=BROKERS, linger_ms=20, batch_size=65536, acks=1)
        topics = [f"in-{safe(j)}-{self.run_id}" for j in JOBS]
        pad = b""
        rng = random.Random(f"{self.s.name}-keys")
        self.pacer = Pacer(*(BURST or (0.0, 1.0)))
        self.pacer.due(0.0, time.time())
        next_log = time.time()
        with open(self.out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t", "target_rate", "agg_cost_us", "hot_fraction", "sent_total"])
            while True:
                now = time.time()
                t0 = self.clock.t0
                if t0 is not None and now >= t0 + self.s.duration:
                    break
                # The scenario's changes happen `shift` seconds later than written, so that a controller's
                # decision cycle does not meet them at the same phase in every repetition.
                t = 0.0 if t0 is None else max(0.0, now - t0 - self.shift)
                rate, cost, hot = self.s.rate(t), self.s.agg_cost(t), self.s.hot(t)
                n = self.pacer.due(rate, now)
                ms = int(now * 1000)
                for _ in range(n):
                    if hot > 0 and rng.random() < hot:
                        # Hot keys differ per repetition, so they land on different subtasks each time.
                        key = f"hot{self.rep}x{rng.randrange(HOT_KEYS)}"
                    else:
                        key = f"k{rng.randrange(NUM_KEYS)}"
                    value = f"{self.sent},{key},{ms},{cost},{MEASURE_COST}".encode()
                    if RECORD_BYTES > len(value) + 1:
                        if len(pad) != RECORD_BYTES:
                            pad = b"," + b"x" * RECORD_BYTES
                        value += pad[:RECORD_BYTES - len(value)]
                    part = self.sent % PARTITIONS
                    for topic in topics:
                        producer.send(topic, value=value, partition=part)
                    self.sent += 1
                if now >= next_log:
                    w.writerow([round(now, 2), round(rate, 1), cost, hot, self.sent])
                    f.flush()
                    next_log = now + 1.0
                time.sleep(max(0.0, 0.05 - (time.time() - now)))
        producer.flush(timeout=30)
        producer.close()


class LatencyCollector(threading.Thread):
    """Reads the per-second latency summaries both jobs write to their output topics."""

    def __init__(self, run_id, out_path):
        super().__init__(daemon=True)
        self.run_id, self.out_path = run_id, out_path
        self.stop_flag = threading.Event()

    def run(self):
        topics = {f"out-{safe(j)}-{self.run_id}": j for j in JOBS}
        consumer = KafkaConsumer(
            *topics.keys(), bootstrap_servers=BROKERS, auto_offset_reset="earliest",
            enable_auto_commit=False, consumer_timeout_ms=1000, group_id=None)
        with open(self.out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["job", "t", "subtask", "count", "p50_ms", "p95_ms", "p99_ms", "max_ms"])
            quiet = 0
            while quiet < 3:
                got = False
                for msg in consumer:
                    got = True
                    p = msg.value.decode().split(",")
                    w.writerow([topics[msg.topic], round(int(p[0]) / 1000, 1)] + p[1:])
                f.flush()
                quiet = quiet + 1 if (self.stop_flag.is_set() and not got) else 0
        consumer.close()


class Sampler(threading.Thread):
    """Samples parallelism, task metrics, lag and pods for both jobs."""

    FIELDS = ["t", "job", "state", "tm_pods", "end_offset", "committed_offset", "position_offset",
              "pending_records", "fetch_rate"] + [
        f"{m}_{v}" for v in VERTICES for m in ("p", "target_p", "busy", "backpressure", "in_rate", "out_rate")]

    def __init__(self, run_id, job_ids, out_path):
        super().__init__(daemon=True)
        self.run_id, self.job_ids, self.out_path = run_id, dict(job_ids), out_path
        self.stop_flag = threading.Event()
        self.pending_metric = {}
        self.offset_metrics = {}
        # The last three (state, Kafka lag) observations per job, for the warm-up drain check.
        self.recent = {job: deque(maxlen=3) for job in JOBS}
        # Per job: its last (state, parallelisms) and when that last changed.
        self.shape = {job: None for job in JOBS}
        self.changed = {job: time.time() for job in JOBS}

    def settled(self, seconds):
        """True when every job has been running at an unchanged parallelism for `seconds`: stabilization
        interval plus metric window, so that every autoscaler starts with a full window."""
        now = time.time()
        return all(self.shape[job] is not None and self.shape[job][0] == "RUNNING"
                   and now - self.changed[job] >= seconds for job in JOBS)

    def drained(self, limit):
        """True when every job is running and its Kafka lag has been at most `limit` for three samples."""
        return all(len(r) == 3 and all(state == "RUNNING" and lag is not None and lag <= limit for state, lag in r)
                   for r in self.recent.values())

    def lags(self):
        return {job: (r[-1][1] if r else None) for job, r in self.recent.items()}

    def sizes(self):
        """(Aggregate, Measure) parallelism of each job from its newest sample, None when not known."""
        out = {}
        for job, shape in self.shape.items():
            size = tuple(shape[1 + VERTICES.index(v)] for v in ("aggregate", "measure")) if shape else (None,)
            out[job] = None if None in size else size
        return out

    @staticmethod
    def vertex_key(name):
        n = name.lower()
        return "source" if n.startswith("source") else "aggregate" if n.startswith("aggregate") else "measure"

    def offsets(self, job, consumer, admin):
        """Kafka end and committed offsets of one job (the Kafka clients are not thread-safe)."""
        row = {"job": job, "state": "UNKNOWN", "t": round(time.time(), 1)}
        topic = f"in-{safe(job)}-{self.run_id}"
        tps = [TopicPartition(topic, p) for p in range(PARTITIONS)]
        try:
            row["end_offset"] = sum(consumer.end_offsets(tps).values())
            committed = admin.list_consumer_group_offsets(f"bench-{safe(job)}-{self.run_id}")
            row["committed_offset"] = sum(v.offset for v in committed.values() if v.offset >= 0)
        except Exception:
            pass
        return row

    def sample(self, row):
        """Adds what Kubernetes and Flink report about the job to its row."""
        job = row["job"]
        try:
            pods = http_json(f"/api/v1/namespaces/{NS}/pods?labelSelector=app%3Dbench-{safe(job)}%2Ccomponent%3Dtaskmanager")
            row["tm_pods"] = sum(1 for p in pods["items"] if p["status"].get("phase") == "Running"
                                 and not p["metadata"].get("deletionTimestamp"))
        except Exception:
            pass
        try:
            d = http_json(f"/apis/flink.apache.org/v1beta1/namespaces/{NS}/flinkdeployments/bench-{safe(job)}")
            jid = d.get("status", {}).get("jobStatus", {}).get("jobId") or self.job_ids.get(job)
            self.job_ids[job] = jid
            # The parallelism the autoscaler last asked for, per vertex id.
            targets = {}
            try:
                cm = http_json(f"/api/v1/namespaces/{NS}/configmaps/autoscaler-bench-{safe(job)}")
                raw = cm.get("data", {}).get("parallelismOverrides", "")
                targets = dict(re.findall(r"([0-9a-f]{32})\W+(\d+)", raw))
            except Exception:
                pass
            detail = flink(job, f"/jobs/{jid}")
            row["state"] = detail["state"]
            for v in detail["vertices"]:
                k = self.vertex_key(v["name"])
                row[f"p_{k}"] = v["parallelism"]
                row[f"target_p_{k}"] = targets.get(v["id"], "")
                base = f"/jobs/{jid}/vertices/{v['id']}/subtasks/metrics"
                names = "busyTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond"
                if k == "source":
                    if job not in self.pending_metric or len(self.offset_metrics.get(job, [])) < PARTITIONS:
                        ids = [m["id"] for m in flink(job, base)]
                        match = [i for i in ids if i.endswith("pendingRecords")]
                        if match:
                            self.pending_metric[job] = match[0]
                        # One gauge per partition with the offset of the last record the reader emitted.
                        self.offset_metrics[job] = [i for i in ids if i.endswith(".currentOffset")]
                        self.offset_metrics[job] += [i for i in ids if i.endswith("KafkaConsumer.fetch-rate")]
                    if job in self.pending_metric:
                        names += "," + self.pending_metric[job]
                    if self.offset_metrics.get(job):
                        names += "," + ",".join(self.offset_metrics[job])
                position, partitions_seen = 0, 0
                if k == "source":
                    # The first request only makes Flink fetch fresh metrics from the TaskManagers; the
                    # values are read after the fetch has had time to finish. The source comes first,
                    # so the other vertices are read from the same fresh snapshot.
                    flink(job, base + "?get=numRecordsOutPerSecond")
                    time.sleep(METRIC_REFRESH_S)
                # Flink's REST server accepts a request line of 4096 characters: one gauge per partition
                # in a single request is too long from about 40 partitions on.
                metrics, chunk = [], []
                for name in names.split(",") + [None]:
                    if name is None or sum(len(n) + 1 for n in chunk) + len(name) > 2000:
                        metrics += flink(job, base + "?get=" + urllib.request.quote(",".join(chunk), safe=","))
                        chunk = []
                    if name is not None:
                        chunk.append(name)
                for m in metrics:
                    if m["id"] == "busyTimeMsPerSecond":
                        row[f"busy_{k}"] = round(m["avg"] / 1000, 3)
                    elif m["id"] == "backPressuredTimeMsPerSecond":
                        row[f"backpressure_{k}"] = round(m["avg"] / 1000, 3)
                    elif m["id"] == "numRecordsInPerSecond":
                        row[f"in_rate_{k}"] = round(m["sum"], 1)
                    elif m["id"] == "numRecordsOutPerSecond":
                        row[f"out_rate_{k}"] = round(m["sum"], 1)
                    elif m["id"].endswith("pendingRecords"):
                        row["pending_records"] = round(m["sum"])
                    elif m["id"].endswith(".currentOffset"):
                        # Each partition is read by one subtask. After a (re)start the gauge is -1
                        # until the reader has emitted a record of the partition; such a sample has
                        # no position (counting it as 0 would show the whole topic as backlog).
                        if m["max"] >= 0:
                            position += round(m["max"]) + 1
                            partitions_seen += 1
                    elif m["id"].endswith("fetch-rate"):
                        row["fetch_rate"] = round(m["sum"], 2)
                if k == "source" and partitions_seen == PARTITIONS:
                    row["position_offset"] = position
        except Exception:
            pass
        return row

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=BROKERS, group_id=None)
        admin = KafkaAdminClient(bootstrap_servers=BROKERS)
        with open(self.out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.FIELDS, extrasaction="ignore")
            w.writeheader()
            with ThreadPoolExecutor(max_workers=len(JOBS)) as pool:
                while not self.stop_flag.is_set():
                    tick = time.time()
                    rows = [self.offsets(job, consumer, admin) for job in JOBS]
                    for row in pool.map(self.sample, rows):
                        w.writerow(row)
                        have = "end_offset" in row and "committed_offset" in row
                        lag = row["end_offset"] - row["committed_offset"] if have else None
                        self.recent[row["job"]].append((row["state"], lag))
                        shape = (row["state"],) + tuple(row.get(f"p_{v}") for v in VERTICES)
                        if shape != self.shape[row["job"]]:
                            self.shape[row["job"]], self.changed[row["job"]] = shape, time.time()
                    f.flush()
                    self.stop_flag.wait(max(0.0, SAMPLE_INTERVAL - (time.time() - tick)))
        consumer.close()
        admin.close()


def start_proxy():
    proc = subprocess.Popen(
        ["kubectl", "--context", CTX, "proxy", f"--port={PROXY_PORT}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(40):
        try:
            http_json("/version", timeout=1)
            return proc
        except Exception:
            time.sleep(0.25)
    proc.kill()
    raise RuntimeError(f"kubectl proxy did not start on port {PROXY_PORT}")


def cluster_answers(tries=3, wait_s=20):
    """True if the API server answers. Asked after a failed run, so that a campaign stops when the
    testbed is gone instead of trying every remaining run against it (Issue #90)."""
    for i in range(tries):
        try:
            r = subprocess.run(["kubectl", "--context", CTX, "get", "--raw", "/readyz", "--request-timeout=10s"],
                               capture_output=True, text=True, timeout=30)
            if r.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, OSError):
            pass
        if i + 1 < tries:
            time.sleep(wait_s)
    return False


def wait_for_hours():
    """Hold the next run until the local time is inside BENCH_HOURS ("18-07": from 18:00 to 07:00).

    Unset means no limit. The VM's clock steps, which fail tasks, come while the machine is in other
    use (Issue #91), so the campaigns can be kept to the hours in which it is idle. Only the start of
    a run is held; a run that has started is finished."""
    spec = os.environ.get("BENCH_HOURS", "").strip()
    if not spec:
        return
    start, end = (int(x) for x in spec.split("-"))
    said = False
    while True:
        hour = time.localtime().tm_hour
        inside = (start <= hour < end) if start < end else (hour >= start or hour < end)
        if inside:
            return
        if not said:
            print(f"    outside BENCH_HOURS={spec}, waiting: {time.strftime('%F %T')}", flush=True)
            said = True
        time.sleep(60)


def run_scenario(name, rep, out_root, jev_overrides=None):
    global JOBS
    s = scenarios.get(name)
    free_gb = shutil.disk_usage(ROOT).free / 2**30
    if free_gb < MIN_FREE_GB:
        raise SystemExit(f"only {free_gb:.1f} GiB of disk free; at least {MIN_FREE_GB} GiB is needed (Issue #13)")
    run_id = f"{name.replace('_', '')}{rep}-{int(time.time()) % 100000}"
    # Per-run randomness that is the same for every job: when the scenario's changes happen, and
    # the order in which the jobs are deployed and written to.
    rng = random.Random(f"{name}-{rep}-run")
    shift = round(rng.uniform(0, JITTER), 1) if JITTER else 0.0
    order = list(JOBS)
    rng.shuffle(order)
    JOBS = tuple(order)
    out = os.path.join(out_root, name, f"rep{rep}")
    os.makedirs(out, exist_ok=True)
    print(f"=== {name} rep {rep} (run id {run_id}) -> {out}", flush=True)

    proxy = start_proxy()
    try:
        delete_deployments()
        admin = KafkaAdminClient(bootstrap_servers=BROKERS)
        admin.create_topics(
            [NewTopic(f"in-{safe(j)}-{run_id}", PARTITIONS, 1,
                      topic_configs={"message.timestamp.type": "LogAppendTime"}) for j in JOBS]
            + [NewTopic(f"out-{safe(j)}-{run_id}", 1, 1) for j in JOBS])
        admin.close()

        started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for job in JOBS:
            manifest = json.dumps(deployment(job, run_id, jev_overrides, s))
            subprocess.run(["kubectl", "--context", CTX, "apply", "-f", "-"], input=manifest,
                           text=True, check=True, capture_output=True)
        job_ids = wait_running()
        print(f"    jobs running: {job_ids}", flush=True)

        base = time.time()
        clock = Clock()
        load = LoadGenerator(s, run_id, clock, os.path.join(out, "load.csv"), rep, shift)
        sampler = Sampler(run_id, job_ids, os.path.join(out, "samples.csv"))
        latency = LatencyCollector(run_id, os.path.join(out, "latency.csv"))
        for th in (load, sampler, latency):
            th.start()
        # Warm-up: at least the scenario's warm-up time, then until every job has worked off the
        # backlog it built while scaling up to the warm-up load (Issue #11), at most DRAIN_MAX longer.
        # The steady reading is about 10 s of input, because offsets are committed per checkpoint.
        # It also waits until no job has restarted or rescaled for SETTLE_S (Issue #31).
        # Every run records whether the jobs begin the measured phase at the scenario's size
        # (Issue #146). With --start-wait the warm-up also waits for that, inside the same bound.
        limit = s.rate(0) * 20 + 1000
        initial = {j: initial_parallelism(s, autoscaler_conf(j, jev_overrides)) for j in JOBS}
        wanted = {j: start_sizes(j, s, jev_overrides) for j in JOBS}  # differs from `initial` for +aggN and +off

        def ready():
            at_size = start_state(sampler.sizes(), wanted)["at_scenario_size"]
            return jobs_ready(sampler.drained(limit), sampler.settled(SETTLE_S), at_size, START_WAIT)

        while load.is_alive() and keep_warming(time.time() - base, s.warmup, DRAIN_MAX, ready()):
            time.sleep(5)
        drained, settled, lag_at_t0 = sampler.drained(limit), sampler.settled(SETTLE_S), sampler.lags()
        start_at_t0 = start_state(sampler.sizes(), wanted)
        start_at_t0["waited_for"] = START_WAIT
        clock.t0 = t0 = time.time() + 5
        print(f"    warm-up {t0 - base:.0f}s, drained={drained}, settled={settled}, shift={shift}s, lag={lag_at_t0}, "
              f"at scenario size={start_at_t0['at_scenario_size']}, equal={start_at_t0['equal']}, sizes={start_at_t0['sizes']}", flush=True)
        fault_times = None
        if FAULT:
            kind, start, length = FAULT
            on = t0 + (s.events[0] if s.events else 0) + shift + start
            timers = [threading.Timer(max(0, on - time.time()), set_fault, [kind]),
                      threading.Timer(max(0, on + length - time.time()), set_fault, [None])]
            for timer in timers:
                timer.daemon = True
                timer.start()
            fault_times = {"kind": kind, "from": round(on - t0, 1), "until": round(on + length - t0, 1)}
        while load.is_alive():
            load.join(timeout=30)
            print(f"    t={time.time() - t0:7.1f}s sent={load.sent}", flush=True)
        if load.error:
            raise load.error
        # Give the jobs a moment to finish what is in flight, then stop collecting.
        time.sleep(15)
        sampler.stop_flag.set()
        latency.stop_flag.set()
        sampler.join(timeout=30)
        latency.join(timeout=30)

        shift_times(os.path.join(out, "load.csv"), t0, 2)
        shift_times(os.path.join(out, "samples.csv"), t0, 1)
        shift_times(os.path.join(out, "latency.csv"), t0, 1)

        # A run is only valid if it was sampled the whole time. Gaps mean the machine slept or
        # the cluster stalled; such a run is discarded and repeated on the next invocation.
        with open(os.path.join(out, "samples.csv")) as f:
            times = {}
            for row in csv.DictReader(f):
                times.setdefault(row["job"], []).append(float(row["t"]))
        # The load must also have reached the broker: with large records the generator can fall
        # behind, and the jobs then see less load than the scenario says (Issue #26).
        with open(os.path.join(out, "samples.csv")) as f:
            delivered = max((float(r["end_offset"]) for r in csv.DictReader(f) if r["end_offset"]), default=0.0)
        if delivered < 0.97 * load.sent:
            raise RuntimeError(f"only {delivered:.0f} of {load.sent} records reached the broker: the load generator fell behind")
        rows = sum(len(v) for v in times.values())
        expected = (t0 - base + s.duration) / SAMPLE_INTERVAL * len(JOBS)
        gap = max((b - a for v in times.values() for a, b in zip(v, v[1:])), default=float("inf"))
        if rows < 0.7 * expected or gap > 30:
            raise RuntimeError(f"{rows} of about {expected:.0f} samples, largest gap {gap:.0f}s (did the machine sleep?)")

        log = kubectl("-n", "flink-operator", "logs", "deployment/flink-kubernetes-operator",
                      f"--since-time={started_utc}", "--tail=-1", check=False)
        with open(os.path.join(out, "decisions.jsonl"), "w") as f:
            for line in log.splitlines():
                i = line.find("JEV_DECISION ")
                if i >= 0:
                    f.write(line[i + len("JEV_DECISION "):] + "\n")
        with open(os.path.join(out, "operator.log"), "w") as f:
            f.write(log)
        events = kubectl("-n", NS, "get", "events", "-o", "json", check=False)
        with open(os.path.join(out, "events.json"), "w") as f:
            f.write(events)
        # Restart counts and the reason a container last ended (an out-of-memory kill leaves no event).
        pods = kubectl("-n", NS, "get", "pods", "-o", "json", check=False)
        with open(os.path.join(out, "pods.json"), "w") as f:
            f.write(pods)
        meta = {
            "scenario": name, "title": s.title, "rep": rep, "run_id": run_id,
            "t0_epoch": t0, "duration": s.duration, "warmup": round(t0 - base), "warmup_planned": s.warmup,
            "drained": drained, "settled": settled, "lag_at_t0": lag_at_t0, "events": [e + shift for e in s.events], "shift": shift,
            "initial_parallelism": initial, "start": start_at_t0, "burst": list(BURST) if BURST else None,
            "metric_refresh": METRIC_REFRESH_S,
            "source_parallelism": SOURCE_PARALLELISM, "record_bytes": RECORD_BYTES, "partitions": PARTITIONS,
            "fault": fault_times,
            "records_sent": load.sent, "started_utc": started_utc, "fetch_bytes": FETCH_BYTES,
            "jobmanager_memory": JM_CONTAINER_MEMORY, "jobmanager_heap": JM_HEAP,
            "autoscaler": COMMON_AUTOSCALER, "jev": {**JEV_AUTOSCALER, **(jev_overrides or {})},
            "jobs": list(JOBS), "llm": LLM_AUTOSCALER if "llm" in JOBS else None,
            "jobs_conf": {j: autoscaler_conf(j, jev_overrides) for j in JOBS},
        }
        with open(os.path.join(out, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)
    finally:
        if FAULT:
            set_fault(None)
        delete_deployments()
        proxy.kill()
        try:  # the topics of a run are not needed afterwards and would fill the broker's disk
            admin = KafkaAdminClient(bootstrap_servers=BROKERS)
            admin.delete_topics([f"{d}-{safe(j)}-{run_id}" for d in ("in", "out") for j in JOBS])
            admin.close()
        except Exception as e:
            print(f"    could not delete the run's topics: {e}", flush=True)
    print(f"=== done {name} rep {rep}", flush=True)


def main():
    global JOBS, FETCH_BYTES, SOURCE_PARALLELISM, RECORD_BYTES, PARTITIONS, JITTER, FAULT, START_WAIT, BURST
    # On macOS, keep the machine awake for the whole benchmark; a sleeping laptop freezes the cluster.
    if sys.platform == "darwin" and not os.environ.get("BENCH_CAFFEINATED"):
        os.environ["BENCH_CAFFEINATED"] = "1"
        os.execvp("caffeinate", ["caffeinate", "-ims", sys.executable] + sys.argv)
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run one scenario once")
    r.add_argument("--scenario", required=True, help="one of %s, or randomN / randomNx" % ", ".join(sorted(SCENARIOS)))
    r.add_argument("--rep", type=int, default=1)
    r.add_argument("--out", default=os.path.join(ROOT, "results"))
    a = sub.add_parser("all", help="run every scenario several times")
    a.add_argument("--reps", type=int, default=3)
    a.add_argument("--scenarios", default="ramp,spike,scale_down,skew,capacity")
    a.add_argument("--out", default=os.path.join(ROOT, "results"))
    for sp in (r, a):
        sp.add_argument("--jobs", default="builtin,jev",
                        help="job specs to run side by side, comma separated, e.g. builtin+max70,rule,jev,llm "
                             "(see SPEC in this file)")
        sp.add_argument("--source-parallelism", type=int, default=None,
                        help="fix the source parallelism of every job and exclude the source from autoscaling")
        sp.add_argument("--record-bytes", type=int, default=0, help="pad record values to this many bytes")
        sp.add_argument("--partitions", type=int, default=PARTITIONS, help="partitions of every input topic")
        sp.add_argument("--fault", default=None, metavar="KIND,START,LENGTH",
                        help="make every model call fail (error) or block (hang) from START seconds after the "
                             "scenario's first change, for LENGTH seconds, e.g. hang,0,180")
        sp.add_argument("--windows", choices=["short", "long"], default="short",
                        help="long: 5-minute metric window, 2-minute stabilization, 10-minute scale-down interval, "
                             "5-minute catch-up, and the plugin's cooldown and look-ahead scaled alike")
        sp.add_argument("--jitter", type=int, default=0,
                        help="delay the scenario's changes by a random time of up to this many seconds per run")
        sp.add_argument("--llm-model", default=LLM_AUTOSCALER["job.autoscaler.jev.llm.model"])
        sp.add_argument("--burst", default=None, metavar="PERIOD_S:DUTY",
                        help="send every period's records in the first DUTY share of the period, at the same "
                             "average rate, e.g. 1:0.2")
        sp.add_argument("--start-wait", action="store_true",
                        help="the warm-up also waits until every job runs at the scenario's size, inside its usual "
                             "bound; without it the state is only recorded in meta.json as 'start'")
        sp.add_argument("--fetch-bytes", type=int, default=None,
                        help="max.partition.fetch.bytes of the jobs' Kafka consumers (default: Kafka's 1 MiB); "
                             "a small value such as 4096 keeps Flink's pendingRecords close to the real lag")
    args = ap.parse_args()
    JOBS = tuple(args.jobs.split(","))
    SOURCE_PARALLELISM, RECORD_BYTES = args.source_parallelism, args.record_bytes
    PARTITIONS, JITTER, START_WAIT = args.partitions, args.jitter, args.start_wait
    if args.burst:
        try:
            BURST = parse_burst(args.burst)
        except ValueError as e:
            raise SystemExit(str(e))
    if args.windows == "long":
        COMMON_AUTOSCALER.update({
            "job.autoscaler.metrics.window": "5m", "job.autoscaler.stabilization.interval": "2m",
            "job.autoscaler.scale-down.interval": "10m", "job.autoscaler.catch-up.duration": "5m"})
        for conf in (JEV_AUTOSCALER, RULE_AUTOSCALER, LLM_AUTOSCALER):
            conf.update({"job.autoscaler.jev.cooldown.scale-down": "750s",
                         "job.autoscaler.jev.trend-horizon-seconds": "420"})
    if args.fault:
        kind, start, length = args.fault.split(",")
        FAULT = (kind, float(start), float(length))
    bad = [j for j in JOBS if not SPEC.fullmatch(j)]
    if bad or len(set(JOBS)) != len(JOBS):
        raise SystemExit(f"unknown or repeated job in --jobs {args.jobs}")
    LLM_AUTOSCALER["job.autoscaler.jev.llm.model"] = args.llm_model
    FETCH_BYTES = args.fetch_bytes

    if args.cmd == "run":
        run_scenario(args.scenario, args.rep, args.out)
    else:
        for rep in range(1, args.reps + 1):
            for name in args.scenarios.split(","):
                done = os.path.join(args.out, name, f"rep{rep}", "meta.json")
                if os.path.exists(done):
                    print(f"=== skip {name} rep {rep} (already done)", flush=True)
                    continue
                wait_for_hours()
                try:
                    run_scenario(name, rep, args.out)
                except Exception as e:
                    print(f"!!! {name} rep {rep} failed: {e}", flush=True)
                    if not cluster_answers():
                        raise SystemExit(f"the cluster {CTX} does not answer; stopping the campaign (Issue #90)")


if __name__ == "__main__":
    main()
