"""The state of the jobs when the measured phase of a run begins (ISSUES.md #146).

A run compares jobs that start equal. The warm-up check of the harness waits for a drained backlog and
a quiet period; it did not look at the size the jobs run with, so jobs that had scaled up in the
warm-up started a measured phase larger than the scenario says, and unequal."""


def start_state(sizes, initial):
    """sizes: job -> (Aggregate, Measure) parallelism at the newest sample, or None without a sample.
    initial: job -> the parallelism the scenario starts the job with.
    Returns the sizes, whether every job is at its scenario size, and whether all known sizes are the same."""
    known = [tuple(v) for v in sizes.values() if v is not None]
    return {
        "sizes": {job: (list(v) if v is not None else None) for job, v in sizes.items()},
        "at_scenario_size": all(sizes.get(job) is not None and tuple(sizes[job]) == tuple(want)
                                for job, want in initial.items()),
        "equal": len(known) == len(sizes) and len(set(known)) <= 1,
    }


def keep_warming(elapsed, warmup, drain_max, ready):
    """True while the warm-up has to go on: always for the scenario's warm-up time, after that until
    the jobs are ready, and never longer than `drain_max` seconds past the warm-up time."""
    return elapsed < warmup or (not ready and elapsed < warmup + drain_max)


def jobs_ready(drained, settled, at_scenario_size, wait_for_size):
    """The warm-up's end condition. The size is waited for only when the run asks for it, so that the
    warm-up of campaigns that ran before this check stays what it was."""
    return bool(drained and settled and (at_scenario_size or not wait_for_size))
