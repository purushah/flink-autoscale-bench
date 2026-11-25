"""cluster/bench/rescalestop.py: how long the output stops at each change of size (the time of a restore)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import rescalestop  # noqa: E402


def files(tmp_path, out_seconds, samples):
    """out_seconds: the seconds in which job a emitted; samples: (t, state, p_aggregate, p_measure)."""
    lat = tmp_path / "latency.csv"
    lat.write_text("job,t,subtask,count,p50_ms,p95_ms,p99_ms,max_ms\n" + "".join(f"a,{s}.5,0,10,100,200,300,400\n" for s in out_seconds))
    sam = tmp_path / "samples.csv"
    sam.write_text("t,job,state,p_aggregate,p_measure\n" + "".join(f"{t},a,{st},{pa},{pm}\n" for t, st, pa, pm in samples))
    return str(lat), str(sam)


def test_the_stop_that_begins_at_a_change_of_size_is_its_length(tmp_path):
    # the job runs at 2/1 until 20 s, is away for 12 s, and runs at 4/1 from 35 s on
    quiet = set(range(21, 33))
    lat, sam = files(tmp_path, [s for s in range(60) if s not in quiet],
                     [(t, "RUNNING", 2, 1) for t in range(0, 21, 5)] + [(25, "CREATED", 2, 1), (30, "CREATED", 2, 1)]
                     + [(t, "RUNNING", 4, 1) for t in range(35, 60, 5)])
    assert rescalestop.stops_at_size_changes(lat, sam, "a", 60) == [12]


def test_a_stall_that_began_long_before_the_change_is_not_the_restore(tmp_path):
    # no output from 5 s to 44 s while the job runs at 2/1 (a stall); the size changes at 45 s and the
    # output is away for 4 more seconds after the new size shows
    quiet = set(range(5, 45)) | set(range(47, 51))
    lat, sam = files(tmp_path, [s for s in range(80) if s not in quiet],
                     [(t, "RUNNING", 2, 1) for t in range(0, 41, 5)] + [(t, "RUNNING", 4, 1) for t in range(45, 80, 5)])
    assert rescalestop.stops_at_size_changes(lat, sam, "a", 80) == [4]


def test_a_change_of_size_without_a_stop_counts_as_zero_and_a_job_that_never_changes_has_none(tmp_path):
    lat, sam = files(tmp_path, range(40), [(t, "RUNNING", 2, 1) for t in range(0, 16, 5)] + [(t, "RUNNING", 2, 3) for t in range(20, 40, 5)])
    assert rescalestop.stops_at_size_changes(lat, sam, "a", 40) == [0]
    lat, sam = files(tmp_path, range(40), [(t, "RUNNING", 2, 1) for t in range(0, 40, 5)])
    assert rescalestop.stops_at_size_changes(lat, sam, "a", 40) == []


def test_quartiles_of_a_list():
    assert rescalestop.quartiles([3, 9, 15]) == (3, 9, 15)
    assert rescalestop.quartiles([]) is None


def test_the_time_between_running_at_the_old_and_at_the_new_size(tmp_path):
    # last sample at 2/1 at 20 s, two samples in which the job is not running, first sample at 4/1 at 35 s;
    # the second change shows in the next sample
    lat, sam = files(tmp_path, range(60),
                     [(t, "RUNNING", 2, 1) for t in range(0, 21, 5)] + [(25, "CREATED", 2, 1), (30, "CREATED", 2, 1)]
                     + [(35, "RUNNING", 4, 1), (40, "RUNNING", 4, 1), (45, "RUNNING", 4, 3), (50, "RUNNING", 4, 3)])
    assert rescalestop.seconds_between_sizes(sam, "a", 60) == [15, 5]
    lat, sam = files(tmp_path, range(40), [(t, "RUNNING", 2, 1) for t in range(0, 40, 5)])
    assert rescalestop.seconds_between_sizes(sam, "a", 40) == []


def test_the_report_counts_the_changes_at_which_the_job_was_away_for_longer_than_the_stop_can_be_matched(tmp_path, capsys, monkeypatch):
    # one change takes 15 s between the samples, the other 45 s: the second cannot be timed by the stop
    import json
    d = tmp_path / "camp" / "surge" / "rep1"
    d.mkdir(parents=True)
    files(d, range(120),
          [(t, "RUNNING", 2, 1) for t in range(0, 21, 5)] + [(25, "CREATED", 2, 1), (30, "CREATED", 2, 1)]
          + [(t, "RUNNING", 4, 1) for t in range(35, 56, 5)] + [(t, "CREATED", 4, 1) for t in range(60, 100, 5)]
          + [(t, "RUNNING", 4, 3) for t in range(100, 120, 5)])
    (d / "meta.json").write_text(json.dumps({"jobs": ["a"], "duration": 120}))
    monkeypatch.setattr(sys, "argv", ["rescalestop.py", str(tmp_path / "camp"), "surge"])
    rescalestop.main()
    out = capsys.readouterr().out
    assert "not running for more than 30 s at 1 of 2 changes (45 s)" in out
