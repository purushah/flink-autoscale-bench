"""Pacer: how many records the load generator sends at each step, smooth or in bursts (ISSUES.md #141)."""
import pytest

from pacing import Pacer, parse_burst


def steps(pacer, rate, seconds, step=0.05):
    """Calls the pacer at 0 (the anchor) and then every `step` seconds; returns [(time, records)]."""
    pacer.due(rate, 0.0)
    return [(i * step, pacer.due(rate, i * step)) for i in range(1, round(seconds / step) + 1)]


def test_smooth_pacing_sends_the_rate():
    sent = steps(Pacer(), 1000, 5.0)
    assert len(sent) == 100
    assert abs(sum(n for _, n in sent) - 5000) <= 1


def test_smooth_pacing_sends_at_every_step():
    assert all(n in (49, 50, 51) for _, n in steps(Pacer(), 1000, 5.0))


def test_a_burst_keeps_the_average_rate():
    sent = steps(Pacer(1.0, 0.2), 1000, 10.0)
    assert abs(sum(n for _, n in sent) - 10000) <= 100


def test_a_burst_sends_nothing_after_its_share_of_the_period():
    sent = steps(Pacer(1.0, 0.2), 1000, 10.0)
    late = [(t, n) for t, n in sent if round(t % 1.0, 6) >= 0.2 and round(t % 1.0, 6) < 1.0]
    assert late and all(n == 0 for _, n in late)


def test_a_burst_keeps_the_average_over_a_span_that_ends_inside_a_period():
    sent = steps(Pacer(1.0, 0.2), 1000, 100.6)
    assert abs(sum(n for _, n in sent) - 100600) <= 1006


def test_the_rate_may_change_between_steps():
    p = Pacer()
    p.due(1000, 0.0)
    assert p.due(1000, 1.0) == 1000
    assert p.due(200, 2.0) == 200


def test_parse_burst():
    assert parse_burst("1:0.2") == (1.0, 0.2)


@pytest.mark.parametrize("text, value", [("1:0", "0"), ("1:1.5", "1.5"), ("0.05:0.5", "0.05"), ("fast", "fast")])
def test_parse_burst_names_the_wrong_value(text, value):
    with pytest.raises(ValueError, match=value.replace(".", r"\.")):
        parse_burst(text)


@pytest.mark.parametrize("text, value", [("0.1:0.2", "0.02"), ("1:0.02", "0.02"), ("0.5:0.09", "0.045")])
def test_a_sending_share_shorter_than_the_generators_step_is_refused(text, value):
    # The generator steps every 0.05 s; a share shorter than that may never see a step, or sees one every few seconds.
    with pytest.raises(ValueError, match=value.replace(".", r"\.")):
        parse_burst(text)


@pytest.mark.parametrize("text", ["nan:0.5", "inf:0.5", "1:nan", "1:inf", "-1:0.5"])
def test_a_burst_setting_that_is_not_a_finite_number_is_refused(text):
    with pytest.raises(ValueError):
        parse_burst(text)


def test_the_shortest_share_that_is_allowed():
    assert parse_burst("0.25:0.2") == (0.25, 0.2)
