"""Sampler.sizes(): the Aggregate and Measure parallelism of each job from its newest sample."""
import bench


def sampler(monkeypatch, jobs):
    monkeypatch.setattr(bench, "JOBS", jobs)
    return bench.Sampler("run", {}, "unused.csv")


def test_sizes_of_two_running_jobs(monkeypatch):
    s = sampler(monkeypatch, ("rule", "builtin"))
    s.shape["rule"] = ("RUNNING", 1, 8, 2)
    s.shape["builtin"] = ("RUNNING", 1, 3, 1)
    assert s.sizes() == {"rule": (8, 2), "builtin": (3, 1)}


def test_job_without_a_sample_has_no_size(monkeypatch):
    s = sampler(monkeypatch, ("rule", "builtin"))
    s.shape["builtin"] = ("RUNNING", 1, 3, 1)
    assert s.sizes() == {"rule": None, "builtin": (3, 1)}


def test_job_whose_sample_has_no_parallelism_has_no_size(monkeypatch):
    s = sampler(monkeypatch, ("rule",))
    s.shape["rule"] = ("RESTARTING", None, None, None)
    assert s.sizes() == {"rule": None}
