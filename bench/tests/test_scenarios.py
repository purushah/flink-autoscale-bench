"""The three load shapes added on 23 Nov 2026."""
import pytest

from scenarios import SCENARIOS


@pytest.mark.parametrize("t, rate", [(0, 300), (299, 300), (300, 0), (599, 0), (600, 300), (1199, 300)])
def test_idle_has_five_minutes_without_input(t, rate):
    s = SCENARIOS["idle"]
    assert s.rate(t) == rate and s.duration == 1200 and s.events == (300, 600)


@pytest.mark.parametrize("t, rate", [(0, 300), (90, 450), (180, 300), (270, 750), (450, 525), (720, 750), (900, 1800), (1000, 1800)])
def test_rising_dips_rises_with_three_dips_to_half(t, rate):
    s = SCENARIOS["rising_dips"]
    assert s.rate(t) == pytest.approx(rate) and s.duration == 1080
    assert s.events == (180, 270, 450, 540, 720, 810)


@pytest.mark.parametrize("t, rate", [(0, 300), (239, 300), (240, 1800), (359, 1800), (360, 300), (720, 1800), (1799, 1800), (1800, 300)])
def test_recurring_has_a_two_minute_peak_every_eight_minutes(t, rate):
    s = SCENARIOS["recurring"]
    assert s.rate(t) == rate and s.duration == 1920


def test_recurring_peaks_four_times():
    s = SCENARIOS["recurring"]
    rises = sum(1 for t in range(1, s.duration) if s.rate(t) > s.rate(t - 1))
    assert rises == 4 and s.events == (240, 360, 720, 840, 1200, 1320, 1680, 1800)


def test_the_warm_up_load_is_the_first_load():
    for name in ("idle", "rising_dips", "recurring"):
        assert SCENARIOS[name].rate(-50) == SCENARIOS[name].rate(0) == 300
