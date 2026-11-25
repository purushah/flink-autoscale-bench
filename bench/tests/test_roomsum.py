"""cluster/bench/roomsum.py: the decisions at which the LLM had room above the policy's choice while the
bottleneck was saturated, and what it chose."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import roomsum  # noqa: E402

T0 = 1795590000.0
META = {"t0_epoch": T0, "events": [60, 540], "duration": 660}


def decision(t, job, busy, rule_choice, choice, offered="p3..p16", demand=1500.0, cap=400.0, current=4, name="Aggregate"):
    ts = __import__("datetime").datetime.fromtimestamp(T0 + t, __import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")
    return {"event": "decision", "ts": ts, "job": f"JobKey{{name='bench-{job.replace('+', '-')}'}}", "vertices": {"v1": {
        "name": name, "current": current, "offered": offered, "rule_choice": rule_choice, "choice": f"p{choice}",
        "busy": busy, "demand": demand, "capacity_per_subtask": cap}}}


def write(tmp_path, rows):
    p = tmp_path / "decisions.jsonl"
    p.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return str(p)


def test_an_opportunity_is_a_saturated_bottleneck_with_a_larger_size_on_offer_inside_the_overload(tmp_path):
    p = write(tmp_path, [
        decision(30, "llm", 1.0, 6, 6),                 # before the overload: not counted
        decision(120, "llm", 0.99, 6, 6),               # room, the model stays with the policy
        decision(180, "llm", 0.97, 8, 16),              # room, the model goes above
        decision(240, "llm", 1.0, 16, 16),              # the policy asks for the largest size: no room
        decision(300, "llm", 0.50, 6, 6),               # not saturated
        decision(360, "rule", 1.0, 6, 6),               # another job
        decision(400, "llm", 1.0, 6, 6, name="Measure"),  # not the bottleneck vertex
        decision(600, "llm", 1.0, 6, 6),                # after the overload
    ])
    got = roomsum.opportunities(p, META, "llm")
    assert [(o["t"], o["rule_choice"], o["choice"], o["largest"], o["above"]) for o in got] == [(120, 6, 6, 16, False), (180, 8, 16, 16, True)]


def test_the_prompts_own_example_is_the_narrower_count(tmp_path):
    # believed demand below the capacity of the current size (4 x 400): the example in the prompt
    p = write(tmp_path, [decision(120, "llm", 0.99, 6, 6, demand=1500.0), decision(180, "llm", 0.99, 6, 6, demand=1700.0)])
    got = roomsum.opportunities(p, META, "llm")
    assert [o["prompt_example"] for o in got] == [True, False]


def test_offered_sizes_may_be_a_range_or_a_list(tmp_path):
    assert roomsum.largest_offered("p3..p16") == 16
    assert roomsum.largest_offered("p2,p4,p8") == 8
    assert roomsum.largest_offered("p6") == 6


def test_the_tally_counts_room_the_prompts_condition_and_choices_above_the_policy(tmp_path):
    p = write(tmp_path, [
        decision(30, "llm", 1.0, 6, 6),                              # before the overload
        decision(70, "llm", 0.40, 4, 4, offered="p2..p8", current=2, demand=500.0),   # room, not saturated
        decision(120, "llm", 0.99, 6, 6, demand=1500.0),             # room, the prompt's condition (1500 < 4 x 400), stays
        decision(180, "llm", 0.97, 8, 16, demand=1700.0),            # room, saturated, demand above the capacity, goes above
        decision(240, "llm", 1.0, 16, 16, demand=1500.0),            # condition, no room
        decision(300, "llm+lagfix", 1.0, 6, 8, demand=1700.0),       # another job: room, demand above the capacity, goes above
    ])
    assert roomsum.tally(p, META, "llm") == {"decisions": 4, "larger": 3, "condition": 2, "both": 1, "chose_larger": 1, "chose_larger_both": 0}
    assert roomsum.tally(p, META, "llm+lagfix") == {"decisions": 1, "larger": 1, "condition": 0, "both": 0, "chose_larger": 1, "chose_larger_both": 0}
