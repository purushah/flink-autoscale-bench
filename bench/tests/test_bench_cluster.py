"""The cluster harness (cluster/bench/bench_cluster.py): the parts it does not share with the laptop's."""
import os
import sys
import time
import types

import bench

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import bench_cluster  # noqa: E402
from scenarios import Scenario  # noqa: E402


class NotingProducer:
    """Stands in for the Kafka client: it only notes the time of every record."""
    made = []

    def __init__(self, config):
        self.times = []
        NotingProducer.made.append(self)

    def poll(self, timeout=0):
        return 0

    def produce(self, topic, value=None, partition=None, on_delivery=None):
        self.times.append(time.time())

    def flush(self, timeout=0):
        return 0


def run_generator(monkeypatch, tmp_path, burst, seconds=2):
    NotingProducer.made.clear()
    monkeypatch.setitem(sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=NotingProducer))
    monkeypatch.setattr(bench, "JOBS", ("builtin",))
    monkeypatch.setattr(bench, "BURST", burst)
    clock = bench.Clock()
    clock.t0 = time.time()
    scenario = Scenario(name="short", title="short", duration=seconds, warmup=0, rate=lambda t: 2000)
    gen = bench_cluster.LoadGenerator(scenario, "run", clock, str(tmp_path / "load.csv"))
    gen.start()
    gen.join(timeout=seconds + 5)
    assert gen.error is None
    return gen, NotingProducer.made[0].times


def test_smooth_cluster_load_arrives_all_the_time(monkeypatch, tmp_path):
    gen, times = run_generator(monkeypatch, tmp_path, None)
    assert 0.8 * 4000 <= len(times) <= 1.05 * 4000
    assert len([t for t in times if (t - gen.pacer.start) % 0.5 >= 0.25]) > 0.3 * len(times)


def test_bursty_cluster_load_arrives_in_the_first_share_of_each_period(monkeypatch, tmp_path):
    gen, times = run_generator(monkeypatch, tmp_path, (0.5, 0.2))
    assert 0.8 * 4000 <= len(times) <= 1.05 * 4000
    assert [t for t in times if (t - gen.pacer.start) % 0.5 >= 0.1 + 0.05] == []


def test_the_keepmargin_feature_sets_the_plugin_option(monkeypatch):
    monkeypatch.setattr(bench_cluster, "ARMS", {"rule+d": ["window:900", "capped", "keepmargin"]})
    conf = bench_cluster.arm_conf(None)["rule+d"]
    assert conf == {"job.autoscaler.jev.scale-down.window-seconds": "900",
                    "job.autoscaler.jev.scale-down.window-caps-trend": "true",
                    "job.autoscaler.jev.scale-down.window-keeps-margin": "true"}
