"""cluster/bench/ownfail.py: Test G's rule for runs that disturbed_cluster.py marks."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "cluster", "bench"))
import ownfail  # noqa: E402

TM = "bench-builtin-st4-taskmanager-1-3"
TIMEOUT = {"job": "bench-builtin-st4", "what": "TaskManager gone without a release request", "t_after_first_change_s": 900,
           "why": f"The heartbeat of TaskManager with id {TM} timed out"}


def event(reason, pod):
    return {"reason": reason, "involvedObject": {"name": pod}}


def test_a_run_without_marked_events_is_clean():
    assert ownfail.classify([], [event("Evicted", TM)]) == "clean"


def test_a_heartbeat_timeout_of_a_pod_that_the_cluster_left_alone_is_the_jobs_own_failure():
    assert ownfail.classify([TIMEOUT], [event("Killing", TM), event("JobException", "bench-builtin-st4")]) == "own"


def test_a_heartbeat_timeout_of_an_evicted_pod_disturbs_the_run():
    assert ownfail.classify([TIMEOUT], [event("Evicted", TM)]) == "disturbed"


def test_a_lost_node_or_a_preemption_disturbs_the_run():
    for reason in ("NodeNotReady", "Preempted", "TaintManagerEviction"):
        assert ownfail.classify([TIMEOUT], [event(reason, TM)]) == "disturbed", reason


def test_an_eviction_of_another_pod_does_not_count_against_this_one():
    assert ownfail.classify([TIMEOUT], [event("Evicted", "bench-builtin-lagfix-st4-taskmanager-1-1")]) == "own"


def test_a_replaced_jobmanager_disturbs_the_run():
    replaced = {"job": "bench-builtin-st4", "what": "JobManager pod replaced", "t_after_first_change_s": 30}
    assert ownfail.classify([replaced], []) == "disturbed"
    assert ownfail.classify([TIMEOUT, replaced], []) == "disturbed"


def test_a_taskmanager_that_went_for_another_reason_disturbs_the_run():
    gone = dict(TIMEOUT, why=f"Pod terminated, container termination statuses: [flink-main-container(exitCode=137)] of {TM}")
    assert ownfail.classify([gone], []) == "disturbed"


# --- runs whose jobs were kept on nodes of their own (--apart, the third test with state) ---------------
# First forty-minute run with state: a checkpoint of each job expired, the job failed, the tasks of one
# TaskManager did not leave within the cancellation time, and that TaskManager shut itself down. With
# the two jobs on shared nodes this stays a disturbance (they slow each other's disks). With the jobs
# apart it is the job's own failure.

SHUTDOWN = {"job": "bench-builtin-st1", "what": "TaskManager gone without a release request", "t_after_first_change_s": 2121,
            "why": "The TaskExecutor is shutting down."}
EXPIRED = {"reason": "JobException", "involvedObject": {"name": "bench-builtin-st1"},
           "message": "org.apache.flink.util.FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold. "
                      "The latest checkpoint failed due to Checkpoint expired before completing., view the Checkpoint History tab"}


def test_a_taskmanager_that_shut_itself_down_after_an_expired_checkpoint_is_the_jobs_own_failure_when_the_jobs_are_apart():
    assert ownfail.classify([SHUTDOWN], [EXPIRED, event("Killing", "bench-builtin-st1-taskmanager-1-1")], apart=True) == "own"


def test_the_same_shutdown_disturbs_a_run_whose_jobs_share_nodes():
    assert ownfail.classify([SHUTDOWN], [EXPIRED]) == "disturbed"
    assert ownfail.classify([SHUTDOWN], [EXPIRED], apart=False) == "disturbed"


def test_a_shutdown_without_an_expired_checkpoint_of_that_job_disturbs_the_run():
    assert ownfail.classify([SHUTDOWN], [], apart=True) == "disturbed"
    other = dict(EXPIRED, involvedObject={"name": "bench-builtin-lagfix-st1"})
    assert ownfail.classify([SHUTDOWN], [other], apart=True) == "disturbed"
    another = dict(EXPIRED, message="java.lang.RuntimeException: something else")
    assert ownfail.classify([SHUTDOWN], [another], apart=True) == "disturbed"


def test_a_shutdown_in_a_job_with_an_evicted_taskmanager_disturbs_the_run():
    # The shutdown's text does not name the pod, so any cluster event on a TaskManager of that job counts.
    assert ownfail.classify([SHUTDOWN], [EXPIRED, event("Evicted", "bench-builtin-st1-taskmanager-1-4")], apart=True) == "disturbed"
    # ... and one on a TaskManager of the other job does not (bench-builtin-st1 is no prefix of its name).
    assert ownfail.classify([SHUTDOWN], [EXPIRED, event("Evicted", "bench-builtin-lagfix-st1-taskmanager-1-4")], apart=True) == "own"


def test_a_heartbeat_timeout_counts_as_before_when_the_jobs_are_apart():
    assert ownfail.classify([TIMEOUT], [event("Killing", TM)], apart=True) == "own"
    assert ownfail.classify([TIMEOUT], [event("Evicted", TM)], apart=True) == "disturbed"


def scheduled(pod, node):
    return {"reason": "Scheduled", "involvedObject": {"name": pod}, "message": f"Successfully assigned ns/{pod} to {node}"}


def pod(name, app, node):
    return {"metadata": {"name": name, "labels": {"app": app}}, "spec": {"nodeName": node}}


def test_shared_nodes_are_the_nodes_that_held_pods_of_two_jobs():
    events = [scheduled("bench-builtin-st1-8bf57f475-ft62p", "node-a"), scheduled("bench-builtin-st1-taskmanager-1-1", "node-a"),
              scheduled("bench-builtin-lagfix-st1-6959d65b77-k8l6f", "node-b"), scheduled("bench-builtin-lagfix-st1-taskmanager-1-2", "node-a"),
              scheduled("asbench-driver-697ffdcc4b-bz7xs", "node-a")]
    assert ownfail.shared_nodes(events, []) == ["node-a"]


def test_jobs_on_nodes_of_their_own_share_none():
    events = [scheduled("bench-builtin-st1-8bf57f475-ft62p", "node-a"), scheduled("bench-builtin-st1-taskmanager-1-1", "node-a"),
              scheduled("bench-builtin-lagfix-st1-6959d65b77-k8l6f", "node-b"), scheduled("bench-builtin-lagfix-st1-taskmanager-1-2", "node-c")]
    pods = [pod("bench-builtin-st1-taskmanager-1-9", "bench-builtin-st1", "node-a"),
            pod("asbench-operator-55c447c854-rqp5n", None, "node-b")]
    assert ownfail.shared_nodes(events, pods) == []


def test_the_pod_list_at_the_end_of_the_run_counts_too():
    # Events are kept for an hour; a pod that was placed before that is still in the pod list.
    events = [scheduled("bench-builtin-lagfix-st1-taskmanager-1-2", "node-c")]
    pods = [pod("bench-builtin-st1-taskmanager-1-1", "bench-builtin-st1", "node-c")]
    assert ownfail.shared_nodes(events, pods) == ["node-c"]


# --- runs whose pods each had a node of their own (--alone) -------------------------------------------

def test_a_run_with_its_pods_alone_has_its_jobs_apart():
    assert ownfail.kept_apart({"alone": True}) and ownfail.kept_apart({"apart": True})
    assert not ownfail.kept_apart({}) and not ownfail.kept_apart({"apart": False, "alone": False})


def test_crowded_nodes_hold_two_flink_pods_of_the_harness_at_the_end_of_the_run():
    pods = [pod("bench-builtin-st4-taskmanager-1-1", "bench-builtin-st4", "node-a"),
            pod("bench-builtin-st4-taskmanager-1-2", "bench-builtin-st4", "node-a"),
            pod("bench-builtin-lagfix-st4-taskmanager-1-1", "bench-builtin-lagfix-st4", "node-b"),
            pod("asbench-driver-697ffdcc4b-bz7xs", "asbench-driver", "node-b")]
    assert ownfail.crowded_nodes(pods) == ["node-a"]


def test_pods_alone_crowd_no_node():
    pods = [pod("bench-builtin-st4-taskmanager-1-1", "bench-builtin-st4", "node-a"),
            pod("bench-builtin-st4-5b5c4bbbdd-rnw7n", "bench-builtin-st4", "node-b"),
            pod("asbench-operator-55c447c854-rqp5n", None, "node-a")]
    assert ownfail.crowded_nodes(pods) == []


# --- "held pods of both jobs" means at the same time, and in this run ---------------------------------
# The cluster keeps events for an hour, so a run's events.json also holds the placements of the run
# before it, under the same pod names. And a node that one job has left may later take a pod of the
# other job; nothing is shared then.

def timed(reason, pod, stamp, node=None):
    e = {"reason": reason, "involvedObject": {"name": pod}, "lastTimestamp": stamp}
    if node:
        e["message"] = f"Successfully assigned ns/{pod} to {node}"
    return e


RUN_START = 1795525000  # 2026-11-24T12:56:40Z


def test_a_node_that_one_job_left_before_the_other_came_is_not_shared():
    events = [timed("Scheduled", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:00:00Z", "node-a"),
              timed("Killing", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:10:00Z"),
              timed("Scheduled", "bench-builtin-lagfix-st4-taskmanager-1-5", "2026-11-24T13:10:30Z", "node-a")]
    assert ownfail.shared_nodes(events, [], since=RUN_START) == []
    assert ownfail.crowded_nodes([], events, since=RUN_START) == []


def test_two_jobs_on_a_node_at_the_same_time_share_it():
    events = [timed("Scheduled", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:00:00Z", "node-a"),
              timed("Scheduled", "bench-builtin-lagfix-st4-taskmanager-1-5", "2026-11-24T13:05:00Z", "node-a"),
              timed("Killing", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:10:00Z")]
    assert ownfail.shared_nodes(events, [], since=RUN_START) == ["node-a"]
    assert ownfail.crowded_nodes([], events, since=RUN_START) == ["node-a"]


def test_placements_of_the_run_before_do_not_count():
    events = [timed("Scheduled", "bench-builtin-lagfix-st4-taskmanager-1-1", "2026-11-24T12:10:00Z", "node-a"),  # the run before
              timed("Scheduled", "bench-builtin-st4-taskmanager-1-1", "2026-11-24T12:58:00Z", "node-a")]
    assert ownfail.shared_nodes(events, [], since=RUN_START) == []
    assert ownfail.crowded_nodes([], events, since=RUN_START) == []


def test_two_pods_of_one_job_on_a_node_at_the_same_time_crowd_it_but_do_not_share_it():
    events = [timed("Scheduled", "bench-builtin-st4-taskmanager-1-1", "2026-11-24T12:58:00Z", "node-a"),
              timed("Scheduled", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:02:00Z", "node-a")]
    assert ownfail.shared_nodes(events, [], since=RUN_START) == []
    assert ownfail.crowded_nodes([], events, since=RUN_START) == ["node-a"]


def test_a_pod_of_the_pod_list_is_on_its_node_from_its_creation():
    late = {"metadata": {"name": "bench-builtin-lagfix-st4-taskmanager-1-9", "creationTimestamp": "2026-11-24T13:40:00Z"},
            "spec": {"nodeName": "node-a"}}
    gone = [timed("Scheduled", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:00:00Z", "node-a"),
            timed("Killing", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:10:00Z")]
    assert ownfail.shared_nodes(gone, [late], since=RUN_START) == []
    still = [timed("Scheduled", "bench-builtin-st4-taskmanager-1-2", "2026-11-24T13:00:00Z", "node-a")]
    assert ownfail.shared_nodes(still, [late], since=RUN_START) == ["node-a"]
