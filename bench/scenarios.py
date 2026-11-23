"""Workload scenarios.

Each scenario is a set of functions of t (seconds since the measured phase started). During the
warm-up, t is negative and the value at t=0 is used, so every run starts from a settled job.

  rate(t)      records per second sent to each job
  agg_cost(t)  simulated work per record in the Aggregate vertex, in microseconds.
               One subtask can process 1e6 / agg_cost records per second, so raising the cost
               is how a scenario lowers worker capacity.
  hot(t)       fraction of records that go to the few "hot" keys (0 = uniform keys)
"""

import math
from dataclasses import dataclass
from typing import Callable

BASE_AGG_COST = 2000  # microseconds -> about 500 records/s per Aggregate subtask
MEASURE_COST = 300  # microseconds -> about 3300 records/s per Measure subtask
NUM_KEYS = 2000
HOT_KEYS = 4


@dataclass
class Scenario:
    name: str
    title: str
    duration: int  # seconds of the measured phase
    warmup: int  # seconds of warm-up load before it
    rate: Callable[[float], float]
    agg_cost: Callable[[float], int] = lambda t: BASE_AGG_COST
    hot: Callable[[float], float] = lambda t: 0.0
    # Times (seconds) at which the workload changes; used for recovery-time metrics.
    events: tuple = ()


def _ramp(t):
    if t <= 0:
        return 300
    if t >= 240:
        return 1800
    return 300 + (1800 - 300) * t / 240


def _spike(t):
    return 2000 if 60 <= t < 180 else 400


def _scale_down(t):
    return 1800 if t < 60 else 300


def _cycle(t):
    return 1100 - 800 * math.cos(2 * math.pi * max(t, 0) / 900)


_STAIRS = (300, 700, 1100, 1500, 1900, 1500, 1100, 700, 300)


def _stairs(t):
    return _STAIRS[min(max(int(t // 180), 0), len(_STAIRS) - 1)]


SCENARIOS = {
    s.name: s
    for s in [
        Scenario(
            name="ramp",
            title="Ramp: 300 to 1800 rec/s over 4 min, then hold",
            duration=360,
            warmup=180,
            rate=_ramp,
            events=(0,),
        ),
        Scenario(
            name="spike",
            title="Spike: 400 to 2000 rec/s for 2 min, then back",
            duration=420,
            warmup=180,
            rate=_spike,
            events=(60, 180),
        ),
        Scenario(
            name="scale_down",
            title="Scale-down: 1800 rec/s dropping to 300",
            duration=360,
            warmup=270,
            rate=_scale_down,
            events=(60,),
        ),
        Scenario(
            name="skew",
            title="Skew: 900 rec/s, 60% of records move to 4 hot keys",
            duration=360,
            warmup=180,
            rate=lambda t: 900,
            hot=lambda t: 0.6 if t >= 60 else 0.0,
            events=(60,),
        ),
        Scenario(
            name="capacity",
            title="Worker capacity: per-record cost doubles for 3 min at 800 rec/s",
            duration=420,
            warmup=180,
            rate=lambda t: 800,
            agg_cost=lambda t: BASE_AGG_COST * 2 if 60 <= t < 240 else BASE_AGG_COST,
            events=(60, 240),
        ),
        # The same ramp and spike with two more minutes at the end, for runs in which a restart
        # takes much longer (restart-cost sensitivity).
        Scenario(
            name="ramp_long",
            title="Ramp: 300 to 1800 rec/s over 4 min, then hold for 4 min",
            duration=480,
            warmup=180,
            rate=_ramp,
            events=(0,),
        ),
        Scenario(
            name="spike_long",
            title="Spike: 400 to 2000 rec/s for 2 min, then back, 6 min to recover",
            duration=540,
            warmup=180,
            rate=_spike,
            events=(60, 180),
        ),
        # A sustained overload that needs more than one doubling: the job starts at one Aggregate
        # subtask and the load rises twelvefold for eight minutes, so that the autoscaler decides
        # several times while the job is backpressured (the backlog-signal study, closed loop).
        Scenario(
            name="surge",
            title="Surge: 200 to 2400 rec/s for 8 min, then back",
            duration=660,
            warmup=180,
            rate=lambda t: 2400 if 60 <= t < 540 else 200,
            events=(60, 540),
        ),
        # The smallest of the three sustained overloads: one step of the rule is enough.
        Scenario(
            name="surge_low",
            title="Small surge: 200 to 600 rec/s for 8 min, then back",
            duration=660,
            warmup=180,
            rate=lambda t: 600 if 60 <= t < 540 else 200,
            events=(60, 540),
        ),
        # A smaller sustained overload, whose right size (5 by the rule's formula) is below the
        # maximum of 8: does a correct backlog make the rule ask for more than it needs?
        Scenario(
            name="surge_mid",
            title="Moderate surge: 200 to 1200 rec/s for 8 min, then back",
            duration=660,
            warmup=180,
            rate=lambda t: 1200 if 60 <= t < 540 else 200,
            events=(60, 540),
        ),
        # The same overload with five-minute windows (bench.py --windows long): 40 minutes of it, and
        # a warm-up that gives every job a full metric window before the load rises.
        Scenario(
            name="surge_long",
            title="Surge, long windows: 200 to 2400 rec/s for 40 min, then back",
            duration=2880,
            warmup=480,
            rate=lambda t: 2400 if 120 <= t < 2520 else 200,
            events=(120, 2520),
        ),
        # Open-loop runs for the backlog-signal study: the job does not scale (Aggregate fixed at 2
        # subtasks, 1000 rec/s), and the load exceeds its capacity by a factor rho for 3 minutes.
        *[
            Scenario(
                name=f"regime{int(rho * 10)}",
                title=f"Open loop: 400 to {int(rho * 1000)} rec/s for 3 min on a job fixed at 1000 rec/s",
                duration=330,
                warmup=60,
                rate=(lambda r: lambda t: r * 1000 if 30 <= t < 210 else 400)(rho),
                events=(30, 210),
            )
            for rho in (1.5, 2.0, 3.0)
        ],
        # Load shapes that other autoscaling studies test with and that the scenarios above do not
        # have (campaign "lit"). Rates stay inside the range the generator has been run at.
        # A compressed daily cycle: two periods of 15 minutes, starting at the trough.
        Scenario(
            name="cycle",
            title="Daily cycle: 300 to 1900 rec/s and back, twice, 15 min each",
            duration=1800,
            warmup=180,
            rate=_cycle,
            events=(0, 450, 900, 1350),
        ),
        # Steps up and then down, three minutes on each level.
        Scenario(
            name="stairs",
            title="Staircase: 300 to 1900 rec/s in steps of 400, then down again, 3 min per step",
            duration=1620,
            warmup=180,
            rate=_stairs,
            events=tuple(180 * i for i in range(1, 9)),
        ),
        # A load that switches between two levels faster than a rescale pays off.
        Scenario(
            name="onoff",
            title="On-off: 400 and 1600 rec/s, 2 min each, six times",
            duration=1440,
            warmup=180,
            rate=lambda t: 1600 if int(max(t, 0) // 120) % 2 == 1 else 400,
            events=tuple(120 * i for i in range(1, 12)),
        ),
        # The upstream stops for two minutes and then sends what it held back in one minute.
        Scenario(
            name="replay",
            title="Outage and replay: 800 rec/s, nothing for 2 min, then 2400 rec/s for 1 min",
            duration=600,
            warmup=180,
            rate=lambda t: 0 if 60 <= t < 180 else (2400 if 180 <= t < 240 else 800),
            events=(60, 180, 240),
        ),
        # A constant load for 20 minutes: every rescale in it is one too many. By the rule's
        # formula 1050 rec/s needs exactly 3 subtasks at a target of 70%.
        Scenario(
            name="steady",
            title="Steady: 1050 rec/s for 20 min",
            duration=1200,
            warmup=180,
            rate=lambda t: 1050,
        ),
        # Short run used only to check that the whole pipeline works.
        Scenario(
            name="smoke",
            title="Smoke test: 400 to 1200 rec/s",
            duration=150,
            warmup=120,
            rate=lambda t: 1200 if t >= 20 else 400,
            events=(20,),
        ),
    ]
}


def random_scenario(seed, duration=1500, stretch=1.0):
    """A workload drawn at random, for testing on workloads nobody tuned for.

    The load holds a level drawn uniformly from 300 to 2000 rec/s for 2 to 6 minutes, then moves to
    the next level either at once or as a ramp of 30 to 120 s. Independently, in each segment the
    per-record cost of the Aggregate vertex is raised by a factor of 1.5 to 2 with probability 0.2,
    and 60% of the records go to four hot keys with probability 0.2. `stretch` multiplies all times
    (used with longer autoscaler windows). The same seed always gives the same workload.
    """
    import random

    rng = random.Random(f"random-workload-{seed}")
    t, level = 0.0, rng.uniform(300, 900)
    segments = []  # (start, end of transition, hold until, from level, to level, cost factor, hot fraction)
    while t < duration:
        hold = rng.uniform(120, 360) * stretch
        ramp = rng.uniform(30, 120) * stretch if rng.random() < 0.5 else 0.0
        nxt = rng.uniform(300, 2000)
        cost = rng.uniform(1.5, 2.0) if rng.random() < 0.2 else 1.0
        hot = 0.6 if rng.random() < 0.2 else 0.0
        segments.append((t, t + ramp, t + ramp + hold, level, nxt, cost, hot))
        t, level = t + ramp + hold, nxt
    first = segments[0]

    def at(x):
        for seg in segments:
            if x < seg[2]:
                return seg
        return segments[-1]

    def rate(x):
        if x <= 0:
            return first[3]
        start, ramp_end, _, a, b, _, _ = at(x)
        return b if x >= ramp_end else a + (b - a) * (x - start) / (ramp_end - start)

    return Scenario(
        name=f"random{seed}" + ("x" if stretch != 1.0 else ""),
        title=f"Random workload {seed}: {len(segments)} load levels between 300 and 2000 rec/s",
        duration=int(duration),
        warmup=180,
        rate=rate,
        # During the warm-up (x <= 0) cost and keys are the plain ones.
        agg_cost=lambda x: int(BASE_AGG_COST * (at(x)[5] if x > 0 else 1.0)),
        hot=lambda x: at(x)[6] if x > 0 else 0.0,
        events=tuple(round(seg[0], 1) for seg in segments),
    )


def levels_scenario(seed, hold=240, unit=300, n=8):
    """A random order of n rate levels (1x to nx of `unit` rec/s), each held for `hold` seconds, with
    an abrupt change between them. This is the test design of the learned parallelism tuners
    (ContTune, StreamTune, Scarf: ten levels in random order), scaled to this testbed: level k needs
    about k Aggregate subtasks. The same seed always gives the same order."""
    import random

    rng = random.Random(f"levels-{seed}")
    order = [unit * k for k in range(1, n + 1)]
    rng.shuffle(order)
    return Scenario(
        name=f"levels{seed}",
        title=f"Rate levels {seed}: " + ", ".join(str(v) for v in order) + f" rec/s, {hold // 60} min each",
        duration=hold * n,
        warmup=180,
        rate=lambda t: order[min(max(int(t // hold), 0), n - 1)],
        events=tuple(hold * i for i in range(1, n)),
    )


def walk_scenario(seed, minutes=25, start=1100, step=400, low=200, high=2200):
    """A random walk: once per minute the rate moves by up to `step` rec/s either way and stays
    between `low` and `high` (the random pattern of the comparative study of stream autoscalers by
    Siachamis et al., scaled to this testbed). The same seed always gives the same walk."""
    import random

    rng = random.Random(f"walk-{seed}")
    rates = [float(start)]
    for _ in range(minutes - 1):
        rates.append(min(high, max(low, rates[-1] + rng.uniform(-step, step))))
    rates = [round(r) for r in rates]
    return Scenario(
        name=f"walk{seed}",
        title=f"Random walk {seed}: a new rate every minute between {min(rates)} and {max(rates)} rec/s",
        duration=60 * minutes,
        warmup=180,
        rate=lambda t: rates[min(max(int(t // 60), 0), minutes - 1)],
    )


def get(name):
    """A scenario by name. `randomN` is the random workload with seed N (25 minutes); `randomNx` is
    the same workload stretched four times in time (100 minutes cut to 60) for long windows.
    `levelsN` and `walkN` are the rate-level and random-walk workloads with seed N."""
    if name in SCENARIOS:
        return SCENARIOS[name]
    for prefix, make in (("levels", levels_scenario), ("walk", walk_scenario)):
        if name.startswith(prefix) and name[len(prefix):].isdigit():
            return make(int(name[len(prefix):]))
    if name.startswith("random") and name[6:].rstrip("x").isdigit():
        seed = int(name[6:].rstrip("x"))
        return random_scenario(seed, 3600, 4.0) if name.endswith("x") else random_scenario(seed)
    raise KeyError(name)

