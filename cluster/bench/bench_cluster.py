#!/usr/bin/env python3
"""Cluster version of bench/bench.py: the same scenarios, job specs and measurements, run from a pod
inside the cluster under test.

It imports the laptop harness (bench.py, unchanged) and replaces only what a shared, multi-node
cluster needs:

  - it runs in the cluster, so no tunnel and no laptop are in the loop. It talks to the API server
    with the pod's service account and to each JobManager through its service;
  - everything cluster-specific comes from one JSON file (CLUSTER_CONF): namespace, Kafka address,
    images, service accounts, extra Flink settings (metric reporter, state storage) and extra parts
    of the pod template (the platform's sidecars);
  - it only ever touches objects that carry its own label, because the namespace has other jobs;
  - --scale K multiplies every rate, the partitions, the keys, the key groups and the largest
    parallelism by K;
  - the load generator uses librdkafka (confluent-kafka), which keeps up at the larger scale.

The jobs report their metrics through the platform's metric reporter. Each run's meta.json has the
UTC window of the measured phase and the job names, which is what a metrics query needs.

Usage (inside the driver pod):
  python3 bench_cluster.py manifest --scenario spike --jobs builtin+max70,rule     # print, create nothing
  python3 bench_cluster.py run --scenario spike --rep 1 --jobs builtin+max70,rule --scale 1
  python3 bench_cluster.py all --reps 3 --scenarios spike,ramp --jobs builtin+max70,rule,jev,llm --scale 4
"""

import argparse
import base64
import copy
import csv
import dataclasses
import json
import math
import os
import random
import socket
import subprocess
import sys
import threading
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
# In the image bench.py and scenarios.py sit next to this file; in the repository they are in bench/.
sys.path[:0] = [_HERE, os.path.join(os.path.dirname(os.path.dirname(_HERE)), "bench")]
import bench  # noqa: E402  the laptop harness
import scenarios  # noqa: E402
from bench import safe  # noqa: E402
from pacing import Pacer, parse_burst  # noqa: E402
from startcheck import jobs_ready, keep_warming, start_state  # noqa: E402

# bench.py resolves names to IPv4 only, for a quirk of the laptop. Not wanted in a cluster.
socket.getaddrinfo = bench._getaddrinfo

LABEL = "asbench"  # every object this harness creates carries asbench=<LABEL_VALUE>
LABEL_VALUE = "true"
KUBE_CONTEXT = "in-cluster"
CONF = {}
SCALE = 1
# Extras of single jobs (--arm JOB=FEATURE,...), the settings they turn into for the current run, and
# whether the run's first load change is planned in advance (--planned). See arm_conf().
ARMS = {}
ARM_CONF = {}
PLANNED = False
# --no-event: the load change that a planned run announces does not happen. The load stays at its level
# before the event, while notes and schedule entries still name the event. For false-alarm runs.
NO_EVENT = False
# "long": the five-minute metric window and its companions (--windows long), written to meta.json.
# "default": the operator's own defaults for the five time settings the harness shortens.
WINDOWS = "short"
# The sustained overload for --windows default: the load rises 23 minutes after the jobs run, so that every
# job has had its stabilization interval (5 minutes) and one full metric window (15 minutes) before, stays
# high for 90 minutes (a decision after a rescale needs 20 minutes), and falls back for the last 8.
scenarios.SCENARIOS["surge_default"] = scenarios.Scenario(
    name="surge_default",
    title="Surge, default windows: 200 to 2400 rec/s for 90 min, then back",
    duration=6000,
    warmup=1260,
    rate=lambda t: 2400 if 120 <= t < 5520 else 200,
    events=(120, 5520),
)
JEV = "job.autoscaler.jev."


def load_conf(path):
    """The cluster-specific settings. Required keys are checked here, so a run never starts half configured."""
    with open(path) as f:
        conf = json.load(f)
    for key in ("namespace", "brokers", "job_image", "job_service_account", "operator_deployment", "state_root"):
        if not conf.get(key):
            raise SystemExit(f"{path}: missing {key}")
    return conf


def write_kubeconfig():
    """A kubeconfig for the pod's service account, so that `kubectl --context` works as on the laptop.
    The token is read from its file on every request, because the kubelet rotates it."""
    sa = "/var/run/secrets/kubernetes.io/serviceaccount"
    if not os.path.exists(f"{sa}/token"):
        return  # not in a pod: use the caller's kubeconfig and context
    host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    if ":" in host:
        host = f"[{host}]"
    path = os.path.expanduser("~/.kube/config")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({
            "apiVersion": "v1", "kind": "Config", "current-context": KUBE_CONTEXT,
            "clusters": [{"name": "in", "cluster": {"server": f"https://{host}:{port}", "certificate-authority": f"{sa}/ca.crt"}}],
            "users": [{"name": "sa", "user": {"tokenFile": f"{sa}/token"}}],
            "contexts": [{"name": KUBE_CONTEXT, "context": {"cluster": "in", "user": "sa", "namespace": CONF["namespace"]}}],
        }, f)


def flink_direct(job, path):
    """Flink's REST API through the JobManager's service (the laptop goes through the API server's proxy)."""
    import urllib.request
    url = f"http://bench-{safe(job)}-rest.{CONF['namespace']}.svc:8081{path}"
    with urllib.request.urlopen(url, timeout=4) as r:
        return json.load(r)


_laptop_deployment = bench.deployment


def deployment(job, run_id, jev_overrides=None, scenario=None):
    """The laptop's FlinkDeployment with the cluster's image, storage, sidecars and metric reporter."""
    d = _laptop_deployment(job, run_id, jev_overrides, scenario)
    d["metadata"]["namespace"] = CONF["namespace"]
    d["metadata"]["labels"] = {LABEL: LABEL_VALUE, "asbench/run": run_id}
    spec = d["spec"]
    spec["image"] = CONF["job_image"]
    spec["imagePullPolicy"] = CONF.get("image_pull_policy", "IfNotPresent")
    spec["serviceAccount"] = CONF["job_service_account"]
    spec["flinkVersion"] = CONF.get("flink_version", spec["flinkVersion"])
    conf = spec["flinkConfiguration"]
    root = CONF["state_root"].rstrip("/")
    conf["state.checkpoints.dir"] = f"{root}/{run_id}/{safe(job)}/checkpoints"
    conf["state.savepoints.dir"] = f"{root}/{run_id}/{safe(job)}/savepoints"
    conf["high-availability.storageDir"] = f"{root}/{run_id}/{safe(job)}/ha"
    conf.update({k: str(v) for k, v in CONF.get("flink_conf", {}).items()})
    if CONF.get("service_name_prefix"):  # the name the job has in the metrics backend
        conf["metrics.reporter.otel.service.name"] = CONF["service_name_prefix"] + safe(job)
    for key in CONF.get("flink_conf_remove", []):
        conf.pop(key, None)
    conf.update(ARM_CONF.get(job, {}))
    if "lagfix" in bench.options(job):
        # The broker-side signal asks the brokers for end offsets itself (cluster/plugin, BrokerLag): the
        # job's settings tell the plugin where they are and how to log in.
        conf[JEV + "lag.bootstrap-servers"] = CONF["brokers"]
        if CONF.get("kafka_auth"):
            conf[JEV + "lag.auth"] = CONF["kafka_auth"]
    # The pod template: the cluster's extra parts (sidecars, volumes, annotations) plus our label.
    template = copy.deepcopy(CONF.get("pod_template", {}))
    meta = template.setdefault("metadata", {})
    meta.setdefault("labels", {})[LABEL] = LABEL_VALUE
    pod = template.setdefault("spec", {})
    containers = pod.setdefault("containers", [])
    if not any(c.get("name") == "flink-main-container" for c in containers):
        containers.insert(0, {"name": "flink-main-container"})
    spec["podTemplate"] = template
    for role in ("jobManager", "taskManager"):
        if role in CONF.get("resources", {}):
            spec[role]["resource"] = CONF["resources"][role]
    spec["job"]["args"] += ["--brokers", CONF["brokers"], "--latency-limit-ms", str(CONF.get("latency_limit_ms", 2000))]
    if CONF.get("sink_guarantee"):  # "none": the latency summaries do not hold up a checkpoint
        spec["job"]["args"] += ["--sink-guarantee", CONF["sink_guarantee"]]
    if CONF.get("kafka_auth") == "msk-iam":
        spec["job"]["args"] += ["--msk-iam", "true"]
    if CONF.get("upgrade_mode"):
        spec["job"]["upgradeMode"] = CONF["upgrade_mode"]
    return d


def kafka_login():
    """Makes the harness's own Kafka clients log in the way the cluster's Kafka asks for.

    msk-iam: Amazon MSK with the pod's IAM role. kafka-python has the mechanism built in (it needs
    botocore); librdkafka takes a token from AWS's signer through its OAuth callback."""
    if CONF.get("kafka_auth") != "msk-iam":
        return {}
    import functools
    import kafka
    sec = {"security_protocol": "SASL_SSL", "sasl_mechanism": "AWS_MSK_IAM"}
    for cls in ("KafkaAdminClient", "KafkaConsumer", "KafkaProducer"):
        setattr(bench, cls, functools.partial(getattr(kafka, cls), **sec))
    region = CONF.get("aws_region") or os.environ.get("AWS_REGION") or os.environ["AWS_DEFAULT_REGION"]
    os.environ.setdefault("AWS_DEFAULT_REGION", region)

    def token(_config):
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
        value, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return value, expiry_ms / 1000

    return {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "oauth_cb": token}


PRODUCER_LOGIN = {}


def our_pods():
    out = bench.kubectl("-n", CONF["namespace"], "get", "pods", "-l", f"{LABEL}={LABEL_VALUE},type=flink-native-kubernetes",
                        "-o", "name", check=False)
    return out.split()


def delete_deployments():
    """Deletes this harness's FlinkDeployments, and only those: the selector is the harness's label."""
    bench.kubectl("-n", CONF["namespace"], "delete", "flinkdeployment", "-l", f"{LABEL}={LABEL_VALUE}",
                  "--wait=true", "--timeout=180s", check=False)
    deadline = time.time() + 240
    while time.time() < deadline:
        if not our_pods():
            return
        time.sleep(3)
    print("WARNING: pods of the benchmark still present after delete", flush=True)


def foreign_bench_objects():
    """FlinkDeployments named bench-* that this harness did not create. The harness refuses to run next to them."""
    raw = bench.kubectl("-n", CONF["namespace"], "get", "flinkdeployment", "-o", "json")
    return [i["metadata"]["name"] for i in json.loads(raw)["items"]
            if i["metadata"]["name"].startswith("bench-") and i["metadata"].get("labels", {}).get(LABEL) != LABEL_VALUE]


class LogSaver(threading.Thread):
    """Follows the logs of this harness's own Flink pods (JobManagers and TaskManagers) into
    <out>/logs/<pod>.<uid>.log while a run lasts. A TaskManager pod is deleted the moment it stops, so
    its last lines can only be kept by following it (ISSUES.md #105). Pod names are used again in the
    next run, hence the uid in the file name. It also follows the test's operator into
    <out>/logs/operator.follow.log: fetched once at the end of a run, the operator's log can have lost
    its first minutes to a rotation (ISSUES.md #109). A follower that ends while its pod still runs is
    started again and appends (ISSUES.md #127). Lines can repeat where a follower was restarted; each
    line carries the time stamp of `kubectl logs --timestamps`, so a repeat is the same line twice."""

    FATAL = b"FATAL - exception in exception handler"

    def __init__(self, out_dir):
        super().__init__(daemon=True)
        self.dir = os.path.join(out_dir, "logs")
        os.makedirs(self.dir, exist_ok=True)
        self.stop_flag = threading.Event()
        self.followers = {}
        self.operator = None

    def follow_operator(self, ns):
        if self.operator is not None and self.operator[0].poll() is None:
            return
        since = "--since=120s"
        if self.operator is not None:
            self.operator[1].close()
            since = "--since=15s"
        f = open(os.path.join(self.dir, "operator.follow.log"), "ab")
        p = subprocess.Popen(
            ["kubectl", "--context", bench.CTX, "-n", ns, "logs", "-f", f"deployment/{CONF['operator_deployment']}",
             "-c", CONF.get("operator_container", "flink-kubernetes-operator"), "--timestamps", since],
            stdout=f, stderr=subprocess.DEVNULL)
        self.operator = (p, f)

    def run(self):
        ns = CONF["namespace"]
        while not self.stop_flag.is_set():
            self.follow_operator(ns)
            listing = bench.kubectl(
                "-n", ns, "get", "pods", "-l", f"{LABEL}={LABEL_VALUE},type=flink-native-kubernetes",
                "--field-selector=status.phase=Running",
                "-o", 'jsonpath={range .items[*]}{.metadata.name} {.metadata.uid}{"\\n"}{end}', check=False)
            for line in listing.splitlines():
                parts = tuple(line.split())
                if len(parts) != 2:
                    continue
                old = self.followers.get(parts)
                if old is not None and old[0].poll() is None:
                    continue
                since = []
                if old is not None:  # the follower ended, the pod still runs: follow again and append
                    old[1].close()
                    since = ["--since=40s"]
                f = open(os.path.join(self.dir, f"{parts[0]}.{parts[1]}.log"), "ab" if old is not None else "wb")
                p = subprocess.Popen(
                    ["kubectl", "--context", bench.CTX, "-n", ns, "logs", "-f", f"pod/{parts[0]}",
                     "-c", "flink-main-container", "--timestamps"] + since, stdout=f, stderr=subprocess.DEVNULL)
                self.followers[parts] = (p, f)
            self.stop_flag.wait(10)

    def close(self):
        self.stop_flag.set()
        for p, f in list(self.followers.values()) + ([self.operator] if self.operator else []):
            if p.poll() is None:
                p.terminate()
            f.close()

    def fatal(self):
        """Pods whose log has the line Flink writes when a task's cleanup throws and the TaskManager stops."""
        hit = []
        for name, uid in sorted(self.followers):
            with open(os.path.join(self.dir, f"{name}.{uid}.log"), "rb") as f:
                if self.FATAL in f.read():
                    hit.append(name)
        return hit


def latency_coverage(path, jobs, duration):
    """Per job: the share of the run's seconds with a latency summary, and the time of the last one. A
    job that a broker stall slowed without stopping it shows up here and nowhere else (ISSUES.md #103)."""
    seconds = {j: set() for j in jobs}
    with open(path) as f:
        for row in csv.DictReader(f):
            t = float(row["t"])
            if row["job"] in seconds and 0 <= t <= duration:
                seconds[row["job"]].add(int(t))
    return ({j: round(len(v) / duration, 3) for j, v in seconds.items()},
            {j: (max(v) if v else None) for j, v in seconds.items()})


class LoadGenerator(threading.Thread):
    """bench.LoadGenerator with librdkafka: the same records, keys, partitions and pacing."""

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
        from confluent_kafka import Producer
        failed = []
        producer = Producer({
            "bootstrap.servers": bench.BROKERS, "linger.ms": 20, "batch.size": 262144, "acks": 1,
            "queue.buffering.max.messages": 2000000, "queue.buffering.max.kbytes": 1048576, **PRODUCER_LOGIN})
        producer.poll(0)  # lets librdkafka fetch its login token before the first record

        def delivered(err, msg):
            if err is not None and len(failed) < 5:
                failed.append(str(err))

        topics = [f"in-{safe(j)}-{self.run_id}" for j in bench.JOBS]
        partitions, keys, hot_keys = bench.PARTITIONS, scenarios.NUM_KEYS * SCALE, scenarios.HOT_KEYS
        measure_cost = scenarios.MEASURE_COST
        rng = random.Random(f"{self.s.name}-keys")
        self.pacer = Pacer(*(bench.BURST or (0.0, 1.0)))
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
                t = 0.0 if t0 is None or NO_EVENT else max(0.0, now - t0 - self.shift)
                rate, cost, hot = self.s.rate(t), self.s.agg_cost(t), self.s.hot(t)
                n = self.pacer.due(rate, now)
                ms = int(now * 1000)
                for _ in range(n):
                    if hot > 0 and rng.random() < hot:
                        key = f"hot{self.rep}x{rng.randrange(hot_keys)}"
                    else:
                        key = f"k{rng.randrange(keys)}"
                    value = f"{self.sent},{key},{ms},{cost},{measure_cost}".encode()
                    part = self.sent % partitions
                    for topic in topics:
                        while True:
                            try:
                                producer.produce(topic, value=value, partition=part, on_delivery=delivered)
                                break
                            except BufferError:
                                producer.poll(0.05)
                    self.sent += 1
                producer.poll(0)
                if failed:
                    raise RuntimeError(f"records were not delivered: {failed[0]}")
                if now >= next_log:
                    w.writerow([round(now, 2), round(rate, 1), cost, hot, self.sent])
                    f.flush()
                    next_log = now + 1.0
                time.sleep(max(0.0, 0.05 - (time.time() - now)))
        left = producer.flush(30)
        if left:
            raise RuntimeError(f"{left} records still unsent after 30 s")


def scaled(s):
    """The scenario with every rate multiplied by SCALE."""
    if SCALE == 1:
        return s
    rate = s.rate
    return dataclasses.replace(s, rate=lambda t: rate(t) * SCALE, title=f"{s.title} (x{SCALE})")


def utc(epoch):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


def plan_event(s, shift):
    """For a run whose first load change is known in advance: its wall-clock time, on a whole minute
    and late enough for the jobs to start and warm up, and the start of the measured phase that
    follows from it. Such a run starts measuring at that moment, drained or not; meta.json says which."""
    first = s.events[0] + shift
    lead_in = int(CONF.get("planned_start_s", 150)) + s.warmup + 60
    event = math.ceil((time.time() + lead_in + first) / 60) * 60
    return {"event_epoch": event, "event_end_epoch": event + (s.events[1] - s.events[0]),
            "t0_epoch": event - first, "event_utc": utc(event),
            "rate_before": round(s.rate(0)), "rate_during": round(s.rate((s.events[0] + s.events[1]) / 2))}


NOTE_KINDS = ("plain", "zone", "relative", "unrelated")


def note_text(planned, kind="plain"):
    """What the job's owner might write about the planned load, as free text:
      plain      the time to the minute in UTC, the rates as numbers
      zone       the same with the time on a 12-hour clock in Pacific Standard Time (UTC-8)
      relative   the same as plain without rates, only how many times the normal level
      unrelated  a note with the same time in it that says nothing about load"""
    at = time.strftime("%H:%M", time.gmtime(planned["event_epoch"]))
    day = time.strftime("%d %B %Y", time.gmtime(planned["event_epoch"])).lstrip("0")
    minutes = (planned["event_end_epoch"] - planned["event_epoch"]) / 60
    rates = (f"Last time the input rate jumped from about {planned['rate_before']:,} to about "
             f"{planned['rate_during']:,} records per second within a few seconds, stayed there for about "
             f"{minutes:g} minutes and then dropped back.")
    if kind == "plain":
        return f"Heads-up from marketing: the push notification for the sale goes out at {at} UTC today ({day}). {rates}"
    if kind == "zone":
        local = time.gmtime(planned["event_epoch"] - 8 * 3600)
        clock = time.strftime("%I:%M %p", local).lstrip("0").lower()
        where = time.strftime("%A, %d %B", local).replace(", 0", ", ")
        return (f"Heads-up from marketing: the push notification for the sale goes out on {where}, at {clock} "
                f"Pacific Standard Time. {rates}")
    if kind == "relative":
        times = planned["rate_during"] / planned["rate_before"]
        return (f"Heads-up from marketing: the push notification for the sale goes out at {at} UTC today ({day}). "
                f"Last time the input rate went up to about {times:g} times its normal level within a few seconds, "
                f"stayed there for about {minutes:g} minutes and then dropped back.")
    if kind == "unrelated":
        return (f"Reminder from the platform team: the dashboards of this job move to the new folder at {at} UTC "
                f"today ({day}), and the on-call rotation changes on Monday. Nothing changes for the job itself.")
    raise SystemExit(f"unknown kind of note: {kind}")


def note_kind(feature):
    """'note' -> 'plain', 'note:zone' -> 'zone'; None for a feature that is not a note."""
    name, _, kind = feature.partition(":")
    return (kind or "plain") if name == "note" else None


def arm_conf(planned):
    """The plugin settings of the jobs that have extras (cluster/plugin):
      note        the planned load as free text for the generative model (llm.notes-base64);
                  note:zone, note:relative and note:unrelated are other wordings, see note_text()
      schedule    the planned load as start, end and rate for the policy in code (schedule)
      history     the generative model also gets the demand of the last half hour and what earlier
                  changes cost (llm.history)
      window:N    the policy in code sizes a scale-down for the highest demand of the last N seconds
      capped      with window:N, the trend projection does not go above that highest demand while
                  the demand is below it (scale-down.window-caps-trend)
      keepmargin  with window:N, a scale-down keeps what that highest demand plus the cap's margin needs
                  (scale-down.window-keeps-margin, ISSUES.md #119)
      minslots:N  a plan that only scales down is dropped unless it frees N slots (scale-down.min-slots)
      stepaware   with the broker-side signal (+lagfix): a jump in the arrival rate that the job does not
                  keep up with is sized at the newest interval's rate (lag.step-aware)
      trend:N     the policy in code projects a rising demand N seconds ahead (trend-horizon-seconds);
                  trend:0 switches the projection off"""
    out = {}
    for job, features in ARMS.items():
        conf = {}
        for f in features:
            if note_kind(f):
                text = note_text(planned, note_kind(f))
                conf[JEV + "llm.notes-base64"] = base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")
            elif f == "schedule":
                conf[JEV + "schedule"] = f"{planned['event_epoch']},{planned['event_end_epoch']},{planned['rate_during']}"
            elif f == "history":
                conf[JEV + "llm.history"] = "true"
            elif f.startswith("window:") and f[7:].isdigit():
                conf[JEV + "scale-down.window-seconds"] = f[7:]
            elif f == "capped":
                conf[JEV + "scale-down.window-caps-trend"] = "true"
            elif f == "keepmargin":
                conf[JEV + "scale-down.window-keeps-margin"] = "true"
            elif f == "stepaware":
                conf[JEV + "lag.step-aware"] = "true"
            elif f.startswith("minslots:") and f[9:].isdigit():
                conf[JEV + "scale-down.min-slots"] = f[9:]
            elif f.startswith("trend:") and f[6:].isdigit():
                conf[JEV + "trend-horizon-seconds"] = f[6:]
            else:
                raise SystemExit(f"unknown feature {f} for {job}")
        out[job] = conf
    return out


def run_scenario(name, rep, out_root, jev_overrides=None):
    """bench.run_scenario for the cluster. The measured phase, the warm-up rule and the validity checks
    are the laptop's; what differs is where things are read from and what is deleted at the end."""
    ns = CONF["namespace"]
    s = scaled(scenarios.get(name))
    run_id = f"{name.replace('_', '')}{rep}-{int(time.time()) % 100000}"
    rng = random.Random(f"{name}-{rep}-run")
    shift = round(rng.uniform(0, bench.JITTER), 1) if bench.JITTER else 0.0
    order = list(bench.JOBS)
    rng.shuffle(order)
    bench.JOBS = tuple(order)
    jobs = bench.JOBS
    out = os.path.join(out_root, name, f"rep{rep}")
    os.makedirs(out, exist_ok=True)
    print(f"=== {name} rep {rep} (run id {run_id}, scale {SCALE}) -> {out}", flush=True)
    foreign = foreign_bench_objects()
    if foreign:
        raise SystemExit(f"FlinkDeployments named bench-* that are not ours: {foreign}; not starting")

    from kafka.admin import NewTopic
    KafkaAdminClient = bench.KafkaAdminClient  # with the cluster's login, see kafka_login()
    replicas = int(CONF.get("replication_factor", 1))
    proxy = bench.start_proxy()
    topics = [f"{d}-{safe(j)}-{run_id}" for d in ("in", "out") for j in jobs]
    logs = LogSaver(out)
    try:
        delete_deployments()
        admin = KafkaAdminClient(bootstrap_servers=bench.BROKERS)
        admin.create_topics(
            [NewTopic(f"in-{safe(j)}-{run_id}", bench.PARTITIONS, replicas,
                      topic_configs={"message.timestamp.type": "LogAppendTime"}) for j in jobs]
            + [NewTopic(f"out-{safe(j)}-{run_id}", 1, replicas) for j in jobs])
        admin.close()

        planned = plan_event(s, shift) if PLANNED else None
        ARM_CONF.clear()
        ARM_CONF.update(arm_conf(planned))
        if planned:
            print(f"    planned: load change at {planned['event_utc']}, measured phase from {utc(planned['t0_epoch'])}", flush=True)
        started_utc = utc(time.time())
        for job in jobs:
            manifest = json.dumps(deployment(job, run_id, jev_overrides, s))
            subprocess.run(["kubectl", "--context", bench.CTX, "apply", "-f", "-"], input=manifest,
                           text=True, check=True, capture_output=True)
        logs.start()
        job_ids = bench.wait_running(timeout=int(CONF.get("start_timeout_s", 900)))
        print(f"    jobs running: {job_ids}", flush=True)
        if bench.SOURCE_PARALLELISM:
            # The source is kept out of autoscaling by its vertex id, which the laptop harness has as
            # a constant. Stop when this Flink version gives the source another id.
            for job, jid in job_ids.items():
                ids = [v["id"] for v in flink_direct(job, f"/jobs/{jid}")["vertices"]]
                if bench.SOURCE_VERTEX not in ids:
                    raise RuntimeError(f"{job}: the source vertex {bench.SOURCE_VERTEX} is not in the job ({ids})")

        base = time.time()
        clock = bench.Clock()
        load = LoadGenerator(s, run_id, clock, os.path.join(out, "load.csv"), rep, shift)
        sampler = bench.Sampler(run_id, job_ids, os.path.join(out, "samples.csv"))
        latency = bench.LatencyCollector(run_id, os.path.join(out, "latency.csv"))
        for th in (load, sampler, latency):
            th.start()
        limit = s.rate(0) * 20 + 1000
        initial = {j: bench.initial_parallelism(s, bench.autoscaler_conf(j, jev_overrides)) for j in jobs}
        wanted = {j: bench.start_sizes(j, s, jev_overrides) for j in jobs}  # differs from `initial` for +aggN and +off

        def ready():
            at_size = start_state(sampler.sizes(), wanted)["at_scenario_size"]
            return jobs_ready(sampler.drained(limit), sampler.settled(bench.SETTLE_S), at_size, bench.START_WAIT)

        if planned:
            # The start is fixed, because the jobs were told when the load changes.
            if planned["t0_epoch"] - base < s.warmup:
                raise RuntimeError(f"the jobs started {planned['t0_epoch'] - base:.0f} s before the planned start, "
                                   f"less than the warm-up of {s.warmup} s")
            while load.is_alive() and time.time() < planned["t0_epoch"] - 5:
                time.sleep(1)
        while not planned and load.is_alive() and keep_warming(time.time() - base, s.warmup, bench.DRAIN_MAX, ready()):
            time.sleep(5)
        drained, settled, lag_at_t0 = sampler.drained(limit), sampler.settled(bench.SETTLE_S), sampler.lags()
        # Whether the jobs begin the measured phase at the scenario's size (ISSUES.md #146). A planned run
        # starts at its fixed moment and only records it.
        start_at_t0 = start_state(sampler.sizes(), wanted)
        start_at_t0["waited_for"] = bool(bench.START_WAIT and not planned)
        clock.t0 = t0 = planned["t0_epoch"] if planned else time.time() + 5
        print(f"    warm-up {t0 - base:.0f}s, drained={drained}, settled={settled}, shift={shift}s, lag={lag_at_t0}, "
              f"at scenario size={start_at_t0['at_scenario_size']}, equal={start_at_t0['equal']}, sizes={start_at_t0['sizes']}", flush=True)
        while load.is_alive():
            load.join(timeout=30)
            print(f"    t={time.time() - t0:7.1f}s sent={load.sent}", flush=True)
        if load.error:
            raise load.error
        time.sleep(15)
        sampler.stop_flag.set()
        latency.stop_flag.set()
        sampler.join(timeout=30)
        latency.join(timeout=30)

        bench.shift_times(os.path.join(out, "load.csv"), t0, 2)
        bench.shift_times(os.path.join(out, "samples.csv"), t0, 1)
        bench.shift_times(os.path.join(out, "latency.csv"), t0, 1)

        with open(os.path.join(out, "samples.csv")) as f:
            rows = list(csv.DictReader(f))
        times = {}
        for row in rows:
            times.setdefault(row["job"], []).append(float(row["t"]))
        delivered = max((float(r["end_offset"]) for r in rows if r["end_offset"]), default=0.0)
        if delivered < 0.97 * load.sent:
            raise RuntimeError(f"only {delivered:.0f} of {load.sent} records reached the broker: the load generator fell behind")
        expected = (t0 - base + s.duration) / bench.SAMPLE_INTERVAL * len(jobs)
        gap = max((b - a for v in times.values() for a, b in zip(v, v[1:])), default=float("inf"))
        if len(rows) < 0.7 * expected or gap > 30:
            raise RuntimeError(f"{len(rows)} of about {expected:.0f} samples, largest gap {gap:.0f}s")

        log = bench.kubectl("-n", ns, "logs", f"deployment/{CONF['operator_deployment']}",
                            "-c", CONF.get("operator_container", "flink-kubernetes-operator"),
                            f"--since-time={started_utc}", "--tail=-1", check=False)
        with open(os.path.join(out, "decisions.jsonl"), "w") as f:
            for line in log.splitlines():
                i = line.find("JEV_DECISION ")
                if i >= 0:
                    f.write(line[i + len("JEV_DECISION "):] + "\n")
        with open(os.path.join(out, "operator.log"), "w") as f:
            f.write(log)
        # Only our own pods and the events about them: the namespace has other jobs.
        pods = bench.kubectl("-n", ns, "get", "pods", "-l", f"{LABEL}={LABEL_VALUE}", "-o", "json", check=False)
        with open(os.path.join(out, "pods.json"), "w") as f:
            f.write(pods)
        try:
            events = json.loads(bench.kubectl("-n", ns, "get", "events", "-o", "json", check=False))
            events["items"] = [e for e in events.get("items", [])
                               if e.get("involvedObject", {}).get("name", "").startswith("bench-")]
        except ValueError:
            events = {"items": []}
        with open(os.path.join(out, "events.json"), "w") as f:
            json.dump(events, f)
        logs.close()
        coverage, last_latency = latency_coverage(os.path.join(out, "latency.csv"), jobs, s.duration)
        fatal = logs.fatal()
        for j in jobs:
            if coverage[j] < 0.9 or last_latency[j] is None or last_latency[j] < s.duration - 30:
                print(f"    WARNING {j}: latency data for {coverage[j]:.0%} of the run, last at {last_latency[j]} s", flush=True)
        if fatal:
            print(f"    WARNING TaskManagers that stopped with a fatal error: {fatal}", flush=True)
        meta = {
            "scenario": name, "title": s.title, "rep": rep, "run_id": run_id, "scale": SCALE,
            "testbed": CONF.get("testbed", "cluster"),
            "t0_epoch": t0, "t0_utc": utc(t0), "end_utc": utc(t0 + s.duration),
            "duration": s.duration, "warmup": round(t0 - base), "warmup_planned": s.warmup,
            "drained": drained, "settled": settled, "lag_at_t0": lag_at_t0,
            "events": [e + shift for e in s.events], "shift": shift,
            "initial_parallelism": initial, "start": start_at_t0, "burst": list(bench.BURST) if bench.BURST else None,
            "metric_refresh": bench.METRIC_REFRESH_S, "partitions": bench.PARTITIONS, "key_groups": bench.KEY_GROUPS,
            "source_parallelism": bench.SOURCE_PARALLELISM, "record_bytes": 0, "fault": None,
            "records_sent": load.sent, "started_utc": started_utc, "fetch_bytes": bench.FETCH_BYTES,
            "jobmanager_memory": bench.JM_CONTAINER_MEMORY, "jobmanager_heap": bench.JM_HEAP,
            "autoscaler": bench.COMMON_AUTOSCALER, "jev": {**bench.JEV_AUTOSCALER, **(jev_overrides or {})},
            "jobs": list(jobs), "llm": bench.LLM_AUTOSCALER if any(j.startswith("llm") for j in jobs) else None,
            "jobs_conf": {j: bench.autoscaler_conf(j, jev_overrides) for j in jobs},
            "flink_deployments": {j: f"bench-{safe(j)}" for j in jobs},
            "arms": ARMS, "arm_conf": {j: sorted(c) for j, c in ARM_CONF.items()},
            "planned": dict(planned, note=note_text(planned)) if planned else None,
            "no_event": NO_EVENT, "windows": WINDOWS,
            "notes": {j: note_text(planned, note_kind(f)) for j, fs in ARMS.items() for f in fs if note_kind(f)} if planned else None,
            "job_image": CONF["job_image"],
            "latency_coverage": coverage, "latency_last_t": last_latency, "taskmanager_fatal": fatal,
        }
        with open(os.path.join(out, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)
    finally:
        logs.close()
        delete_deployments()
        proxy.kill()
        try:
            admin = KafkaAdminClient(bootstrap_servers=bench.BROKERS)
            admin.delete_topics(topics)
            admin.close()
        except Exception as e:
            print(f"    could not delete the run's topics: {e}", flush=True)
    print(f"=== done {name} rep {rep}", flush=True)


def main():
    global CONF, SCALE, PRODUCER_LOGIN, PLANNED, NO_EVENT, WINDOWS
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run one scenario once")
    m = sub.add_parser("manifest", help="print the FlinkDeployments of a run and create nothing")
    a = sub.add_parser("all", help="run every scenario several times")
    for sp in (r, m):
        sp.add_argument("--scenario", required=True)
        sp.add_argument("--rep", type=int, default=1)
    a.add_argument("--reps", type=int, default=3)
    a.add_argument("--scenarios", default="spike,ramp,scale_down")
    for sp in (r, m, a):
        sp.add_argument("--conf", default=os.environ.get("CLUSTER_CONF", "/etc/asbench/cluster.json"))
        sp.add_argument("--out", default=os.environ.get("CLUSTER_OUT", "/results/cluster"))
        sp.add_argument("--jobs", default="builtin+max70,rule")
        sp.add_argument("--scale", type=int, default=1,
                        help="multiply rates, partitions, keys, key groups and the largest parallelism by this")
        sp.add_argument("--jitter", type=int, default=0)
        sp.add_argument("--arm", action="append", default=[], metavar="JOB=FEATURE[,FEATURE]",
                        help="extras of one job: note[:KIND], schedule, history, window:SECONDS, capped, keepmargin, minslots:N (see arm_conf)")
        sp.add_argument("--planned", action="store_true",
                        help="the first load change falls on a whole minute that is fixed before the jobs "
                             "start; needed for note and schedule")
        sp.add_argument("--no-event", action="store_true",
                        help="with --planned: the announced load change does not happen, the load stays at "
                             "its first level (a false alarm)")
        sp.add_argument("--source-parallelism", type=int, default=0,
                        help="fixed source parallelism at scale 1 (multiplied by the scale); 0 = autoscaled")
        sp.add_argument("--fetch-bytes", type=int, default=None)
        sp.add_argument("--llm-model", default=bench.LLM_AUTOSCALER["job.autoscaler.jev.llm.model"])
        sp.add_argument("--burst", default=None, metavar="PERIOD_S:DUTY",
                        help="send every period's records in the first DUTY share of the period, at the same "
                             "average rate, e.g. 1:0.2 (as bench.py)")
        sp.add_argument("--start-wait", action="store_true",
                        help="the warm-up also waits until every job runs at the scenario's size, inside its usual "
                             "bound; without it the state is only recorded in meta.json as 'start' (as bench.py)")
        sp.add_argument("--windows", choices=["short", "long", "default"], default="short",
                        help="long: 5-minute metric window, 2-minute stabilization, 10-minute scale-down interval, "
                             "5-minute catch-up, and the plugin's cooldown and look-ahead scaled alike (as bench.py); "
                             "default: the operator's defaults (15-minute window, 5-minute stabilization, 1-hour "
                             "scale-down interval, 30-minute catch-up, 5-minute restart time), built-in jobs only")
    args = ap.parse_args()

    CONF = load_conf(args.conf)
    SCALE = args.scale
    bench.CTX = CONF.get("context", KUBE_CONTEXT)
    bench.NS = CONF["namespace"]
    bench.BROKERS = CONF["brokers"]
    bench.JOBS = tuple(args.jobs.split(","))
    bad = [j for j in bench.JOBS if not bench.SPEC.fullmatch(j)]
    if bad or len(set(bench.JOBS)) != len(bench.JOBS):
        raise SystemExit(f"unknown or repeated job in --jobs {args.jobs}")
    bench.JITTER, bench.FETCH_BYTES = args.jitter, args.fetch_bytes
    bench.START_WAIT = args.start_wait
    if args.burst:
        try:
            bench.BURST = parse_burst(args.burst)
        except ValueError as e:
            raise SystemExit(str(e))
    PLANNED = args.planned
    NO_EVENT = getattr(args, "no_event", False)
    if NO_EVENT and not PLANNED:
        raise SystemExit("--no-event needs --planned")
    for arm in args.arm:
        job, _, features = arm.partition("=")
        if job not in bench.JOBS or not features:
            raise SystemExit(f"--arm {arm}: the job is not in --jobs, or no feature is given")
        ARMS[job] = features.split(",")
    if not PLANNED and any(f == "schedule" or note_kind(f) for fs in ARMS.values() for f in fs):
        raise SystemExit("note and schedule need --planned")
    bench.SOURCE_PARALLELISM = args.source_parallelism * SCALE or None
    WINDOWS = args.windows
    if WINDOWS == "default":
        # The operator's defaults (AutoScalerOptions of 1.16) for the settings the harness shortens. The
        # policy and the models have cooldowns and a look-ahead that were never set for these, so only
        # jobs with the built-in rule may run.
        if any(j.split("+")[0] != "builtin" for j in bench.JOBS):
            raise SystemExit("--windows default is for jobs with the built-in rule only")
        bench.COMMON_AUTOSCALER.update({
            "job.autoscaler.metrics.window": "15m", "job.autoscaler.stabilization.interval": "5m",
            "job.autoscaler.scale-down.interval": "1h", "job.autoscaler.catch-up.duration": "30m",
            "job.autoscaler.restart.time": "5m"})
    if WINDOWS == "long":  # the same settings as bench.py --windows long
        bench.COMMON_AUTOSCALER.update({
            "job.autoscaler.metrics.window": "5m", "job.autoscaler.stabilization.interval": "2m",
            "job.autoscaler.scale-down.interval": "10m", "job.autoscaler.catch-up.duration": "5m"})
        for conf in (bench.JEV_AUTOSCALER, bench.RULE_AUTOSCALER, bench.LLM_AUTOSCALER):
            conf.update({"job.autoscaler.jev.cooldown.scale-down": "750s",
                         "job.autoscaler.jev.trend-horizon-seconds": "420"})
    bench.LLM_AUTOSCALER["job.autoscaler.jev.llm.model"] = args.llm_model
    bench.PARTITIONS = 8 * SCALE
    bench.KEY_GROUPS = 120 * SCALE
    largest = str(8 * SCALE)
    bench.COMMON_AUTOSCALER["job.autoscaler.vertex.max-parallelism"] = largest
    for conf in (bench.JEV_AUTOSCALER, bench.RULE_AUTOSCALER, bench.LLM_AUTOSCALER):
        conf["job.autoscaler.jev.max-parallelism"] = largest
    bench.flink = flink_direct
    bench.deployment = deployment
    bench.delete_deployments = delete_deployments

    if args.cmd == "manifest":
        s = scaled(scenarios.get(args.scenario))
        ARM_CONF.update(arm_conf(plan_event(s, 0.0) if PLANNED else None))
        print(json.dumps([deployment(j, "RUNID", None, s) for j in bench.JOBS], indent=2))
        return
    write_kubeconfig()
    PRODUCER_LOGIN = kafka_login()
    if args.cmd == "run":
        run_scenario(args.scenario, args.rep, args.out)
        return
    for rep in range(1, args.reps + 1):
        for name in args.scenarios.split(","):
            if os.path.exists(os.path.join(args.out, name, f"rep{rep}", "meta.json")):
                print(f"=== skip {name} rep {rep} (already done)", flush=True)
                continue
            try:
                run_scenario(name, rep, args.out)
            except Exception as e:
                print(f"!!! {name} rep {rep} failed: {e}", flush=True)
                if not bench.cluster_answers():
                    raise SystemExit("the cluster does not answer; stopping the campaign")


if __name__ == "__main__":
    main()
