"""LoadGenerator: the records it hands to Kafka, smooth and in bursts. Kafka is replaced by a producer
that only notes the time of every record; everything else is the harness's own code."""
import time

import bench
from scenarios import Scenario


class NotingProducer:
    def __init__(self, **_):
        self.times = []

    def send(self, topic, value=None, partition=None):
        self.times.append(time.time())

    def flush(self, *_, **__):
        pass

    def close(self, *_, **__):
        pass


def run_generator(monkeypatch, tmp_path, burst, seconds=2):
    made = []
    monkeypatch.setattr(bench, "KafkaProducer", lambda **kw: made.append(NotingProducer()) or made[-1])
    monkeypatch.setattr(bench, "JOBS", ("builtin",))
    monkeypatch.setattr(bench, "BURST", burst)
    clock = bench.Clock()
    clock.t0 = time.time()
    scenario = Scenario(name="short", title="short", duration=seconds, warmup=0, rate=lambda t: 2000)
    gen = bench.LoadGenerator(scenario, "run", clock, str(tmp_path / "load.csv"))
    gen.start()
    gen.join(timeout=seconds + 5)
    assert gen.error is None
    return gen, made[0].times


def test_smooth_load_arrives_all_the_time(monkeypatch, tmp_path):
    gen, times = run_generator(monkeypatch, tmp_path, None)
    assert 0.8 * 4000 <= len(times) <= 1.05 * 4000
    late = [t for t in times if (t - gen.pacer.start) % 0.5 >= 0.25]
    assert len(late) > 0.3 * len(times)


def test_bursty_load_arrives_in_the_first_share_of_each_period(monkeypatch, tmp_path):
    gen, times = run_generator(monkeypatch, tmp_path, (0.5, 0.2))
    assert 0.8 * 4000 <= len(times) <= 1.05 * 4000
    late = [t for t in times if (t - gen.pacer.start) % 0.5 >= 0.1 + 0.05]
    assert late == []
