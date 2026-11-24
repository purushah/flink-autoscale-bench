"""The state of the jobs when the measured phase of a run begins (ISSUES.md #146)."""
from startcheck import jobs_ready, keep_warming, start_state


def test_all_at_scenario_size():
    s = start_state({"rule": (3, 1), "builtin": (3, 1)}, {"rule": (3, 1), "builtin": (3, 1)})
    assert s == {"sizes": {"rule": [3, 1], "builtin": [3, 1]}, "at_scenario_size": True, "equal": True}


def test_scaled_up_in_the_warm_up():
    s = start_state({"rule": (8, 1), "builtin": (6, 1)}, {"rule": (3, 1), "builtin": (3, 1)})
    assert s["at_scenario_size"] is False and s["equal"] is False


def test_equal_but_not_at_scenario_size():
    s = start_state({"rule": (8, 1), "builtin": (8, 1)}, {"rule": (3, 1), "builtin": (3, 1)})
    assert s["at_scenario_size"] is False and s["equal"] is True


def test_job_without_a_sample():
    s = start_state({"rule": None, "builtin": (3, 1)}, {"rule": (3, 1), "builtin": (3, 1)})
    assert s["at_scenario_size"] is False and s["sizes"]["rule"] is None


def test_warm_up_runs_for_the_scenarios_time_even_when_ready():
    assert keep_warming(elapsed=10, warmup=60, drain_max=240, ready=True) is True


def test_warm_up_ends_after_the_scenarios_time_when_ready():
    assert keep_warming(elapsed=61, warmup=60, drain_max=240, ready=True) is False


def test_warm_up_goes_on_while_the_jobs_are_not_ready():
    assert keep_warming(elapsed=61, warmup=60, drain_max=240, ready=False) is True


def test_warm_up_never_passes_its_bound():
    assert keep_warming(elapsed=300, warmup=60, drain_max=240, ready=False) is False


def test_without_the_option_the_size_is_not_waited_for():
    assert jobs_ready(drained=True, settled=True, at_scenario_size=False, wait_for_size=False) is True


def test_with_the_option_jobs_off_the_scenario_size_are_not_ready():
    assert jobs_ready(drained=True, settled=True, at_scenario_size=False, wait_for_size=True) is False


def test_with_the_option_jobs_at_the_scenario_size_are_ready():
    assert jobs_ready(drained=True, settled=True, at_scenario_size=True, wait_for_size=True) is True


def test_a_backlog_or_a_recent_rescale_is_never_ready():
    assert jobs_ready(drained=False, settled=True, at_scenario_size=True, wait_for_size=True) is False
    assert jobs_ready(drained=True, settled=False, at_scenario_size=True, wait_for_size=False) is False
