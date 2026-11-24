"""How many records the load generator sends at each step.

Smooth pacing spreads the rate evenly. A burst sends every period's records during the first `duty`
share of the period and nothing after it, at the same average rate. Bursts are what a broker's end
offsets look like on a replicated topic (ISSUES.md #141), and a producer that can make them lets the
harness test a reading of the arrival rate against them."""
import math

MIN_PERIOD_S = 0.1
STEP_S = 0.05  # the load generators step this often; a sending share shorter than a step may never see one
EDGE = 1e-9  # a step this close to the end of the sending share counts as after it


class Pacer:
    def __init__(self, period_s=0.0, duty=1.0):
        self.period_s, self.duty = float(period_s), float(duty)
        self.carry = 0.0
        self.start = self.last = None

    def _sending_time(self, t):
        """Seconds of the sending share that have passed until `t` seconds after the start."""
        share = self.duty * self.period_s
        return math.floor(t / self.period_s) * share + min(t % self.period_s, share)

    def due(self, rate, now):
        """Records to send at `now` for a target of `rate` records per second. The first call only
        sets the start."""
        if self.last is None:
            self.start = self.last = now
            return 0
        if self.period_s <= 0:
            self.carry += rate * (now - self.last)
            sending = True
        else:
            passed = self._sending_time(now - self.start) - self._sending_time(self.last - self.start)
            self.carry += rate / self.duty * passed
            # What the sending share earns after the last step inside it leaves with the next period.
            sending = (now - self.start) % self.period_s < self.duty * self.period_s - EDGE
        self.last = now
        if not sending:
            return 0
        n = int(self.carry)
        self.carry -= n
        return n


def parse_burst(text):
    """'PERIOD_S:DUTY', for example '1:0.2': one-second periods, records in the first 0.2 s of each."""
    try:
        period, duty = (float(x) for x in text.split(":"))
    except ValueError:
        raise ValueError(f"--burst wants PERIOD_S:DUTY, for example 1:0.2, not {text}") from None
    if not math.isfinite(period) or period < MIN_PERIOD_S:
        raise ValueError(f"--burst: the period must be a number of at least {MIN_PERIOD_S} s, not {text.split(':')[0]}")
    if not 0 < duty <= 1:
        raise ValueError(f"--burst: the duty must be above 0 and at most 1, not {text.split(':')[1]}")
    if duty * period < STEP_S:
        raise ValueError(f"--burst: the sending share, PERIOD_S x DUTY, must be at least {STEP_S} s, the generator's step, "
                         f"not {duty * period:g} s ({text})")
    return period, duty
