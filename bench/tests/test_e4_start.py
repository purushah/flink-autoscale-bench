"""e4_start.at_scenario_size: the harness's own mark when a run has one, else the samples."""
import pandas as pd

from e4_start import at_scenario_size

SAMPLES = pd.DataFrame({"t": [-10.0, 5.0, -10.0, 5.0], "job": ["a", "a", "b", "b"],
                        "p_aggregate": [3.0, 3.0, 3.0, 8.0]})


def test_the_mark_of_the_harness_wins_over_the_samples():
    meta = {"jobs": ["a", "b"], "initial_parallelism": {"a": [3, 1], "b": [3, 1]},
            "start": {"at_scenario_size": True}}
    assert at_scenario_size(meta, SAMPLES) is True


def test_a_run_without_the_mark_is_read_from_its_samples():
    meta = {"jobs": ["a", "b"], "initial_parallelism": {"a": [3, 1], "b": [3, 1]}}
    assert at_scenario_size(meta, SAMPLES) is False
    assert at_scenario_size(dict(meta, jobs=["a"]), SAMPLES) is True
