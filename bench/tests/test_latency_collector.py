"""bench.LatencyCollector: it stops reading when the run ends, also when a job still emits (ISSUES.md #177)."""
import time

import bench


class Msg:
    value = b"1000,0,5,10,20,30,40"

    def __init__(self, topic):
        self.topic = topic


class EndlessConsumer:
    """A consumer whose topic never goes quiet: a job that is still working off its backlog when the run ends."""
    closed = False

    def __init__(self, *topics, **kwargs):
        self.topic = topics[0]

    def __iter__(self):
        return self

    def __next__(self):
        time.sleep(0.001)
        return Msg(self.topic)

    def close(self):
        EndlessConsumer.closed = True


def test_the_collector_stops_within_its_grace_time_when_a_job_still_emits(tmp_path, monkeypatch):
    # Runs 4 and 7 of the test with every pod alone: the collector never saw a quiet second, outlived the
    # 30 s the harness waits for it, and wrote into the file after the harness had rewritten it.
    monkeypatch.setattr(bench, "KafkaConsumer", EndlessConsumer)
    monkeypatch.setattr(bench, "JOBS", ["a"])
    monkeypatch.setattr(bench.LatencyCollector, "GRACE_S", 0.2, raising=False)
    collector = bench.LatencyCollector("run1", str(tmp_path / "latency.csv"))
    collector.start()
    time.sleep(0.2)
    collector.stop_flag.set()
    collector.join(timeout=3)
    assert not collector.is_alive()
    assert EndlessConsumer.closed
    rows = (tmp_path / "latency.csv").read_text().splitlines()
    assert rows[0].startswith("job,t,") and len(rows) > 1 and all(r.count(",") == 7 for r in rows)
