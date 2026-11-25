"""cluster/bench/statesum.py: the summary of the test with real state (the parts that are its own)."""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import statesum  # noqa: E402


def latency_file(tmp_path, rows):
    """rows: (job, t, subtask) or (job, t, subtask, p95_ms)."""
    path = tmp_path / "latency.csv"
    path.write_text("job,t,subtask,count,p50_ms,p95_ms,p99_ms,max_ms\n"
                    + "".join(f"{r[0]},{r[1]},{r[2]},10,100,{r[3] if len(r) > 3 else 200},300,400\n" for r in rows))
    return str(path)


def test_late_seconds_counts_the_seconds_above_the_limit(tmp_path):
    rows = [("a", 0.5, 0, 200), ("a", 1.5, 0, 2500), ("a", 2.5, 0, 2001), ("a", 3.5, 0, 2000)]
    assert statesum.late_seconds(latency_file(tmp_path, rows), "a", 4) == 2


def test_late_seconds_takes_the_worst_subtask_of_a_second(tmp_path):
    rows = [("a", 0.5, 0, 200), ("a", 0.6, 1, 2500), ("a", 1.5, 0, 200), ("a", 1.6, 1, 300)]
    assert statesum.late_seconds(latency_file(tmp_path, rows), "a", 2) == 1


def test_a_gap_of_up_to_three_seconds_takes_the_second_before_it(tmp_path):
    # healthy, then three seconds without output, then healthy: nothing is late
    rows = [("a", 0.5, 0, 200), ("a", 4.5, 0, 200)]
    assert statesum.late_seconds(latency_file(tmp_path, rows), "a", 5) == 0
    # late, then three seconds without output: they are late as well
    rows = [("a", 0.5, 0, 2500), ("a", 4.5, 0, 200)]
    assert statesum.late_seconds(latency_file(tmp_path, rows), "a", 5) == 4


def test_a_longer_gap_is_late_from_its_fourth_second(tmp_path):
    # healthy in second 0, nothing in seconds 1 to 8, healthy in second 9: seconds 4 to 8 are late
    rows = [("a", 0.5, 0, 200), ("a", 9.5, 0, 200)]
    assert statesum.late_seconds(latency_file(tmp_path, rows), "a", 10) == 5


def test_a_run_without_any_output_is_late_throughout(tmp_path):
    assert statesum.late_seconds(latency_file(tmp_path, [("b", 0.5, 0, 200)]), "a", 10) == 10


def test_the_pair_is_the_job_without_and_the_job_with_the_broker_side_signal():
    assert statesum.pair(["builtin+lagfix+st4", "builtin+st4"]) == ("builtin+st4", "builtin+lagfix+st4")


def test_a_run_with_other_jobs_has_no_pair():
    with pytest.raises(ValueError):
        statesum.pair(["builtin+st4", "builtin+st4+a"])
    with pytest.raises(ValueError):
        statesum.pair(["builtin+st4", "builtin+lagfix+st4", "builtin+max70+st4"])


def test_down_seconds_counts_the_seconds_of_the_measured_phase_without_output(tmp_path):
    # Output in seconds 0, 1 and 2, nothing in 3 to 7, output again in 8 and 9.
    rows = [("a", t, 0) for t in (0.2, 1.4, 2.9, 8.1, 9.5)]
    assert statesum.down_seconds(latency_file(tmp_path, rows), "a", 10) == 5


def test_down_seconds_takes_any_subtask_as_output(tmp_path):
    rows = [("a", 0.5, 0), ("a", 1.5, 1), ("a", 2.5, 0)]
    assert statesum.down_seconds(latency_file(tmp_path, rows), "a", 3) == 0


def test_down_seconds_looks_at_one_job_and_at_the_measured_phase_only(tmp_path):
    rows = [("a", -5.0, 0), ("b", 0.5, 0), ("b", 1.5, 0), ("a", 1.5, 0), ("a", 2.5, 0), ("a", 3.5, 0)]
    assert statesum.down_seconds(latency_file(tmp_path, rows), "a", 3) == 1  # second 0 has output of b only


def test_the_stretches_without_output_in_the_order_they_happened(tmp_path):
    rows = [("a", t, 0) for t in (0.2, 1.4, 5.2, 6.1, 9.9)]
    # no output in seconds 2 to 4 and 7 to 8
    assert statesum.down_stretches(latency_file(tmp_path, rows), "a", 10, at_least=1) == [3, 2]
    assert statesum.down_stretches(latency_file(tmp_path, rows), "a", 10) == [3]  # two seconds are not a stop


def test_a_stretch_of_at_most_two_seconds_is_not_a_stop(tmp_path):
    rows = [("a", t, 0) for t in (0.2, 1.4, 4.2, 5.1, 6.0, 7.0, 8.0, 9.9)]
    assert statesum.down_stretches(latency_file(tmp_path, rows), "a", 10, at_least=3) == []
    assert statesum.down_stretches(latency_file(tmp_path, rows), "a", 10, at_least=2) == [2]


def samples_file(tmp_path, rows):
    """rows: (t, state, aggregate, measure) of job a, one sample every 5 s."""
    path = tmp_path / "samples.csv"
    path.write_text("t,job,state,p_source,p_aggregate,p_measure\n"
                    + "".join(f"{t},a,{state},2,{agg},{measure}\n" for t, state, agg, measure in rows))
    return str(path)


def every_5s(start, n, state, agg, measure):
    return [(start + 5 * i, state, agg, measure) for i in range(n)]


def test_rescales_are_the_changes_of_size_of_the_running_job(tmp_path):
    rows = every_5s(0, 6, "RUNNING", 2, 1) + every_5s(30, 6, "RUNNING", 6, 1) + every_5s(60, 6, "RUNNING", 6, 2)
    assert statesum.rescales(samples_file(tmp_path, rows), "a", 100) == {"ups": 1, "rescales": 2, "path": "2->6"}


def test_a_job_that_is_being_created_again_shows_a_size_that_is_not_its_own(tmp_path):
    # While a job with state restarts, Flink reports it as CREATED with the parallelism it was submitted
    # with (2) for longer than the 12 s that the stateless tests' summary takes as a real size.
    rows = every_5s(0, 6, "RUNNING", 16, 2) + every_5s(30, 4, "CREATED", 2, 1) + every_5s(50, 6, "RUNNING", 16, 2)
    assert statesum.rescales(samples_file(tmp_path, rows), "a", 100) == {"ups": 0, "rescales": 0, "path": "16"}


def test_rescales_looks_at_the_measured_phase_only(tmp_path):
    rows = every_5s(-30, 6, "RUNNING", 2, 1) + every_5s(0, 6, "RUNNING", 4, 1) + every_5s(35, 6, "RUNNING", 8, 1)
    assert statesum.rescales(samples_file(tmp_path, rows), "a", 30) == {"ups": 0, "rescales": 0, "path": "4"}


def test_emitted_counts_the_records_that_left_the_job_in_the_measured_phase(tmp_path):
    # latency_file gives every row a count of 10; the row before t0, the row of job b and the row after
    # the end do not count.
    rows = [("a", -1.0, 0), ("a", 0.5, 0), ("a", 0.6, 1), ("a", 1.5, 0), ("b", 1.5, 0), ("a", 3.0, 0)]
    assert statesum.emitted(latency_file(tmp_path, rows), "a", 3) == 30


def test_zero_bytes_and_raw_rows_after_the_measured_phase_do_not_break_the_reading(tmp_path):
    # Run 4 of the test with every pod alone (ISSUES.md #177): a job was still emitting when the run ended,
    # the collector was not stopped in time, and the harness rewrote the file under it. The collector then
    # wrote at its old position: a block of zero bytes, then rows with times that are still epoch seconds.
    rows = [("a", 0.5, 0, 200), ("a", 1.5, 0, 2500), ("a", 2.5, 0, 200)]
    clean = latency_file(tmp_path, rows)
    want = (statesum.late_seconds(clean, "a", 3), statesum.down_seconds(clean, "a", 3), statesum.emitted(clean, "a", 3),
            statesum.down_stretches(clean, "a", 3, at_least=1))
    with open(clean, "ab") as f:
        f.write(bytes(200_000) + b"a,1795539014.0,6,421,448984,493036,601116,603166\r\n"
                + b"a,1795539015.0,6,400,448984,493036,601116,603166\r\n")
    got = (statesum.late_seconds(clean, "a", 3), statesum.down_seconds(clean, "a", 3), statesum.emitted(clean, "a", 3),
           statesum.down_stretches(clean, "a", 3, at_least=1))
    assert got == want == (1, 0, 30, [])


def test_the_shared_reader_of_a_run_takes_a_latency_file_with_a_block_of_zero_bytes(tmp_path):
    # Runs 4 and 7 of the test with every pod alone (ISSUES.md #177): statesum.py reads each run through
    # sigsum.harness first, and that reader stopped at the block of zero bytes.
    (tmp_path / "meta.json").write_text(json.dumps({"duration": 3, "events": [1], "jobs": ["a"]}))
    (tmp_path / "load.csv").write_text("t,target_rate\n0,500\n")
    (tmp_path / "samples.csv").write_text("job,t,state,p_source,p_aggregate,p_measure\n"
                                          "a,0.0,RUNNING,1,1,1\na,2.0,RUNNING,1,1,1\n")
    clean = latency_file(tmp_path, [("a", 0.5, 0, 200), ("a", 1.5, 0, 2500), ("a", 2.5, 0, 200)])
    want = statesum.harness(str(tmp_path))[1]
    with open(clean, "ab") as f:
        f.write(bytes(200_000) + b"a,1795539014.0,6,421,448984,493036,601116,603166\r\n")
    assert statesum.harness(str(tmp_path))[1] == want
    assert want["a"]["over"] == 1


READY = {"drained": True, "settled": True, "start": {"at_scenario_size": True, "equal": True}}


def test_a_run_of_jobs_without_state_begins_as_planned_without_a_state_in_a_checkpoint():
    # The control of Test H (plan entry of 2026-11-24 13:09 PST): the same pair, the jobs hold no state,
    # and the harness writes no state block into meta.json.
    assert statesum.started_ready(dict(READY, state=None)) == (True, [])
    assert statesum.started_ready(READY) == (True, [])


def test_a_run_of_jobs_with_state_still_needs_the_state_in_a_checkpoint():
    assert statesum.started_ready(dict(READY, state={"filled": False})) == (False, ["state filled"])
    assert statesum.started_ready(dict(READY, state={"filled": True})) == (True, [])


def test_a_run_that_did_not_settle_is_not_ready_with_or_without_state():
    assert statesum.started_ready(dict(READY, settled=False)) == (False, ["settled"])


def test_the_state_of_a_run_without_state_is_zero():
    assert statesum.state_sizes({"jobs": ["a", "b"]}) == (0.0, {"a": 0.0, "b": 0.0})
    m = {"jobs": ["a", "b"], "state": {"wanted": {"a": 2**32, "b": 2**32}, "bytes": {"a": 2**31, "b": None}}}
    assert statesum.state_sizes(m) == (4.0, {"a": 2.0, "b": 0.0})


def test_a_named_pair_is_taken_as_given_for_jobs_without_the_signal_option():
    # the comparison of the policy in code with the LLM: reference first, compared job second
    assert statesum.pair(["llm", "rule"], named=("rule", "llm")) == ("rule", "llm")
    assert statesum.pair(["builtin", "builtin+lagfix"]) == ("builtin", "builtin+lagfix")
    import pytest
    with pytest.raises(ValueError):
        statesum.pair(["llm", "rule"], named=("rule", "jev"))
    with pytest.raises(ValueError):
        statesum.pair(["llm", "rule"])
