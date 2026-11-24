"""Jobs with real state (+stN: N GiB of keyed state across the job's keys, kept in RocksDB)."""
import inspect
import os
import sys

import bench
from scenarios import NUM_KEYS
from startcheck import jobs_ready, state_filled

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import bench_cluster  # noqa: E402

GIB = 2**30


def after(args, flag):
    return args[args.index(flag) + 1]


# --- the job spec and what it gives the job ---------------------------------------------------------

def test_the_spec_grammar_knows_st():
    assert bench.SPEC.fullmatch("builtin+lagfix+st8")
    assert bench.options("builtin+st8") == {"st": 8}


def test_state_wanted_is_the_options_gibibytes():
    assert bench.state_wanted("builtin+lagfix+st8") == 8 * GIB
    assert bench.state_wanted("builtin") == 0


def test_st_gives_every_key_its_share_of_the_state():
    args = bench.job_args("builtin+st8", "x")
    assert int(after(args, "--ballast-bytes-per-key")) == 8 * GIB // NUM_KEYS


def test_the_share_follows_the_number_of_keys_the_load_uses(monkeypatch):
    # The cluster's load has twice the keys at twice the scale. With the laptop's number the job built
    # 8 GiB where 4 were asked for (fourth smoke run).
    assert bench.KEYS == NUM_KEYS
    monkeypatch.setattr(bench, "KEYS", 2 * NUM_KEYS)
    args = bench.job_args("builtin+st4", "x")
    assert int(after(args, "--ballast-bytes-per-key")) == 4 * GIB // (2 * NUM_KEYS)


def test_the_cluster_harness_tells_the_laptop_harness_its_number_of_keys():
    source = inspect.getsource(bench_cluster)
    assert "bench.KEYS = scenarios.NUM_KEYS * SCALE" in source
    assert "keys, hot_keys = bench.PARTITIONS, scenarios.NUM_KEYS * SCALE" in source  # what the load itself uses


def test_a_job_without_st_has_no_ballast():
    assert "--ballast-bytes-per-key" not in bench.job_args("builtin", "x")


def test_a_job_with_state_keeps_it_in_rocksdb_with_incremental_checkpoints():
    conf = bench.deployment("builtin+st8", "x")["spec"]["flinkConfiguration"]
    assert conf["state.backend.type"] == "rocksdb"
    assert conf["execution.checkpointing.incremental"] == "true"


def test_a_job_without_state_keeps_the_heap_backend():
    conf = bench.deployment("builtin", "x")["spec"]["flinkConfiguration"]
    assert conf["state.backend.type"] == "hashmap"
    assert "execution.checkpointing.incremental" not in conf


# --- reading the state's size -------------------------------------------------------------------------

def test_checkpoint_bytes_is_the_full_size_of_the_latest_completed_checkpoint():
    doc = {"latest": {"completed": {"id": 7, "state_size": 8_700_000_000, "checkpointed_size": 12_000}}}
    assert bench.checkpoint_bytes(doc) == 8_700_000_000


def test_checkpoint_bytes_is_unknown_without_a_completed_checkpoint():
    assert bench.checkpoint_bytes({"latest": {"completed": None}}) is None
    assert bench.checkpoint_bytes({"latest": {}}) is None
    assert bench.checkpoint_bytes({}) is None


def test_state_bytes_is_a_column_of_the_samples():
    assert "state_bytes" in bench.Sampler.FIELDS


def test_state_bytes_of_each_job_from_its_newest_reading(monkeypatch):
    monkeypatch.setattr(bench, "JOBS", ("builtin+st8", "builtin+lagfix+st8"))
    s = bench.Sampler("run", {}, "unused.csv")
    assert s.state_bytes() == {"builtin+st8": None, "builtin+lagfix+st8": None}
    s.state["builtin+st8"] = 8_700_000_000
    assert s.state_bytes() == {"builtin+st8": 8_700_000_000, "builtin+lagfix+st8": None}


# --- the start check ------------------------------------------------------------------------------------

def test_state_is_filled_when_every_job_has_nearly_all_it_wants():
    assert state_filled({"a": 8_400_000_000, "b": 8_700_000_000}, {"a": 8 * GIB, "b": 8 * GIB})


def test_state_is_not_filled_while_one_job_is_short():
    assert not state_filled({"a": 8_700_000_000, "b": 2_000_000_000}, {"a": 8 * GIB, "b": 8 * GIB})


def test_state_is_not_filled_at_nine_tenths():
    # The keys finish writing their state one after the other; the run waits for nearly all of it.
    assert not state_filled({"a": 8_700_000_000, "b": 8_000_000_000}, {"a": 8 * GIB, "b": 8 * GIB})


def test_a_job_without_a_reading_is_not_filled():
    assert not state_filled({"a": None}, {"a": GIB})
    assert not state_filled({}, {"a": GIB})


def test_jobs_that_want_no_state_are_always_filled():
    assert state_filled({"a": None, "b": 0}, {"a": 0, "b": 0})
    assert state_filled({}, {})


def test_jobs_are_not_ready_before_their_state_is_filled():
    assert not jobs_ready(True, True, True, True, state_filled=False)
    assert jobs_ready(True, True, True, True, state_filled=True)


def test_jobs_ready_is_what_it_was_for_callers_that_do_not_say():
    assert jobs_ready(True, True, False, False)
    assert not jobs_ready(True, False, True, True)


# --- the wiring in both harnesses ---------------------------------------------------------------------

def test_both_harnesses_wait_for_the_state_and_record_it():
    for module in (bench, bench_cluster):
        source = inspect.getsource(module.run_scenario)
        assert "state_filled(sampler.state_bytes(), wanted_state)" in source, module.__name__
        assert '"state": state_at_t0' in source, module.__name__


CLUSTER = {"namespace": "ns", "job_image": "img", "job_service_account": "sa", "state_root": "store://example/root",
           "brokers": "b:9098", "resources": {"taskManager": {"memory": "1024m", "cpu": 0.5}}}


def test_the_cluster_gives_a_job_with_state_cpu_and_disk_of_its_own(monkeypatch):
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    with_state = bench_cluster.deployment("builtin+st8", "x")["spec"]["taskManager"]["resource"]
    assert with_state["cpu"] == 2  # RocksDB and the restores need their own CPU
    assert with_state["ephemeralStorage"] == "48G"  # six times the state
    without = bench_cluster.deployment("builtin", "x")["spec"]["taskManager"]["resource"]
    assert without == {"memory": "1024m", "cpu": 0.5}


def test_the_cluster_leaves_half_of_a_state_jobs_memory_to_the_file_cache(monkeypatch):
    # RocksDB reads and writes its files through the operating system's cache, which counts against the
    # pod's memory. A pod whose memory Flink uses up leaves none, and RocksDB stalls (third smoke run).
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    d = bench_cluster.deployment("builtin+st8", "x")["spec"]
    assert d["taskManager"]["resource"]["memory"] == "6144m"
    conf = d["flinkConfiguration"]
    assert conf["taskmanager.memory.jvm-overhead.min"] == conf["taskmanager.memory.jvm-overhead.max"] == "3072m"
    assert conf["state.backend.rocksdb.thread.num"] == "4"
    plain = bench_cluster.deployment("builtin", "x")["spec"]["flinkConfiguration"]
    assert "taskmanager.memory.jvm-overhead.min" not in plain and "state.backend.rocksdb.thread.num" not in plain


def test_the_cluster_harness_can_be_given_a_longer_wait_for_the_start():
    source = inspect.getsource(bench_cluster)
    assert '"--drain-max"' in source and "bench.DRAIN_MAX = args.drain_max" in source


# --- jobs of one run on nodes of their own (--apart) ---------------------------------------------------
# Found in the first forty-minute run with state (24 Nov): while one job restored its state, the other
# job's TaskManagers on the same node waited for the node's disk three quarters of the time. Two jobs
# that are compared must not share a disk.

def test_jobs_kept_apart_keep_off_the_nodes_of_the_other_jobs_pods(monkeypatch):
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    monkeypatch.setattr(bench_cluster, "APART", True)
    d = bench_cluster.deployment("builtin+st1", "x")
    rules = d["spec"]["podTemplate"]["spec"]["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
    assert len(rules) == 1 and rules[0]["topologyKey"] == "kubernetes.io/hostname"
    asked = {e["key"]: (e["operator"], e["values"]) for e in rules[0]["labelSelector"]["matchExpressions"]}
    # Flink's own pods of this harness (not the driver, not the operator), of every job but this one.
    assert asked == {"asbench": ("In", ["true"]), "type": ("In", ["flink-native-kubernetes"]),
                     "app": ("NotIn", [d["metadata"]["name"]])}


def test_the_two_jobs_of_a_pair_name_each_other_out(monkeypatch):
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    monkeypatch.setattr(bench_cluster, "APART", True)
    names = []
    for job in ("builtin+st1", "builtin+lagfix+st1"):
        d = bench_cluster.deployment(job, "x")
        rule = d["spec"]["podTemplate"]["spec"]["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][0]
        names.append((d["metadata"]["name"], [e["values"] for e in rule["labelSelector"]["matchExpressions"] if e["key"] == "app"][0]))
    assert names[0][0] != names[1][0] and names[0][1] == [names[0][0]] and names[1][1] == [names[1][0]]


def test_jobs_are_not_kept_apart_unless_asked(monkeypatch):
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    assert "affinity" not in bench_cluster.deployment("builtin+st1", "x")["spec"]["podTemplate"]["spec"]


def test_keeping_jobs_apart_keeps_what_the_cluster_asks_of_its_pods(monkeypatch):
    own = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": []}}}
    conf = dict(CLUSTER, pod_template={"spec": {"affinity": dict(own)}})
    monkeypatch.setattr(bench_cluster, "CONF", conf)
    monkeypatch.setattr(bench_cluster, "APART", True)
    affinity = bench_cluster.deployment("builtin+st1", "x")["spec"]["podTemplate"]["spec"]["affinity"]
    assert affinity["nodeAffinity"] == own["nodeAffinity"] and "podAntiAffinity" in affinity
    assert "podAntiAffinity" not in conf["pod_template"]["spec"]["affinity"]  # the cluster's settings are not written to


def test_the_cluster_harness_takes_apart_from_the_command_line_and_records_it():
    source = inspect.getsource(bench_cluster)
    assert '"--apart"' in source and "APART = args.apart" in source and '"apart": APART' in source


def test_the_state_queue_hands_extra_options_to_every_run():
    path = os.path.join(os.path.dirname(os.path.abspath(bench_cluster.__file__)), "queue_state.sh")
    text = open(path).read()
    assert 'EXTRA="${7:-}"' in text and "--drain-max 1200 $EXTRA >>" in text


# --- every pod of the test on a node of its own (--alone) ---------------------------------------------
# Second forty-minute run with state (24 Nov), pressure counters read in the pods: the four TaskManagers
# of one job sat on one node and waited for its disk 60 to 80% of the whole run; the other job's sat on
# four nodes and waited next to never. Which job is packed is the scheduler's doing. Jobs apart is not
# enough: every TaskManager needs a disk of its own.

def test_pods_kept_alone_refuse_every_node_with_another_flink_pod_of_the_harness(monkeypatch):
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    monkeypatch.setattr(bench_cluster, "ALONE", True)
    d = bench_cluster.deployment("builtin+st4", "x")
    rules = d["spec"]["podTemplate"]["spec"]["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
    assert len(rules) == 1 and rules[0]["topologyKey"] == "kubernetes.io/hostname"
    asked = {e["key"]: (e["operator"], e["values"]) for e in rules[0]["labelSelector"]["matchExpressions"]}
    assert asked == {"asbench": ("In", ["true"]), "type": ("In", ["flink-native-kubernetes"])}  # its own job's pods too


def test_alone_is_the_stricter_rule_and_is_asked_once(monkeypatch):
    monkeypatch.setattr(bench_cluster, "CONF", dict(CLUSTER))
    monkeypatch.setattr(bench_cluster, "ALONE", True)
    monkeypatch.setattr(bench_cluster, "APART", True)
    rules = bench_cluster.deployment("builtin+st4", "x")["spec"]["podTemplate"]["spec"]["affinity"]["podAntiAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"]
    assert len(rules) == 1 and all(e["key"] != "app" for e in rules[0]["labelSelector"]["matchExpressions"])


def test_the_cluster_harness_takes_alone_from_the_command_line_and_records_it():
    source = inspect.getsource(bench_cluster)
    assert '"--alone"' in source and "ALONE = args.alone" in source and '"alone": ALONE' in source
