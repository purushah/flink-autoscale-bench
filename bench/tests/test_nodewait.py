"""cluster/bench/nodewait.py: how long the TaskManagers of each job waited for a node and their containers."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import nodewait  # noqa: E402

A, B = "bench-builtin-st4-taskmanager-1-2", "bench-builtin-lagfix-st4-taskmanager-1-2"
START = 1795525000  # 2026-11-24T12:56:40Z


def ev(reason, pod, first, last=None):
    return {"reason": reason, "involvedObject": {"name": pod}, "firstTimestamp": first, "lastTimestamp": last or first}


def test_a_taskmanager_waits_from_its_first_event_to_the_start_of_its_last_container():
    events = [ev("FailedScheduling", A, "2026-11-24T13:10:45Z", "2026-11-24T13:12:10Z"),
              ev("Scheduled", A, "2026-11-24T13:12:22Z"),
              ev("Started", A, "2026-11-24T13:12:40Z", "2026-11-24T13:13:05Z")]  # three containers, the last at :05
    assert nodewait.waits(events, START) == {"bench-builtin-st4": [140]}


def test_a_taskmanager_that_found_a_node_at_once_waits_for_its_containers_only():
    events = [ev("Scheduled", B, "2026-11-24T13:08:41Z"), ev("Started", B, "2026-11-24T13:08:43Z")]
    assert nodewait.waits(events, START) == {"bench-builtin-lagfix-st4": [2]}


def test_waits_are_per_job_in_the_order_the_pods_came():
    events = [ev("Scheduled", B, "2026-11-24T13:08:41Z"), ev("Started", B, "2026-11-24T13:08:43Z"),
              ev("Scheduled", A, "2026-11-24T13:12:22Z"), ev("Started", A, "2026-11-24T13:13:05Z"),
              ev("Scheduled", "bench-builtin-st4-taskmanager-1-3", "2026-11-24T13:00:00Z"),
              ev("Started", "bench-builtin-st4-taskmanager-1-3", "2026-11-24T13:00:10Z")]
    assert nodewait.waits(events, START) == {"bench-builtin-st4": [10, 43], "bench-builtin-lagfix-st4": [2]}


def test_jobmanagers_other_pods_and_the_run_before_do_not_count():
    events = [ev("Scheduled", "bench-builtin-st4-6f5566dbf7-g22mn", "2026-11-24T12:57:31Z"),
              ev("Started", "bench-builtin-st4-6f5566dbf7-g22mn", "2026-11-24T12:57:40Z"),
              ev("Scheduled", "asbench-driver-697ffdcc4b-bz7xs", "2026-11-24T12:58:00Z"),
              ev("Scheduled", A, "2026-11-24T12:10:00Z"), ev("Started", A, "2026-11-24T12:10:30Z")]
    assert nodewait.waits(events, START) == {}


def test_a_taskmanager_whose_containers_never_started_has_no_wait_to_report():
    assert nodewait.waits([ev("FailedScheduling", A, "2026-11-24T13:10:45Z")], START) == {"bench-builtin-st4": [None]}
