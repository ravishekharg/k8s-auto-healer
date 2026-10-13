from datetime import datetime, timedelta, timezone

from kubernetes.client import (V1Container, V1ContainerPort, V1ContainerState, V1ContainerStateTerminated,
                               V1ContainerStateWaiting, V1ContainerStatus, V1ObjectMeta, V1OwnerReference, V1Pod,
                               V1PodCondition, V1PodSpec, V1PodStatus)

from healer import pods as P

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)


def pod(annotations=None, owner=("ReplicaSet", "web-7d9f8c"), ports=(8080,), ready=True, started=300,
        deletion_ago=None, statuses=None):
    return V1Pod(
        metadata=V1ObjectMeta(
            name="web-7d9f8c-abcde", namespace="shop", uid="uid-1", annotations=annotations,
            owner_references=[V1OwnerReference(api_version="apps/v1", kind=owner[0], name=owner[1], uid="o", controller=True)] if owner else None,
            deletion_timestamp=None if deletion_ago is None else NOW - timedelta(seconds=deletion_ago)),
        spec=V1PodSpec(containers=[V1Container(name="app", ports=[V1ContainerPort(container_port=p) for p in ports])]),
        status=V1PodStatus(phase="Running", pod_ip="10.0.0.5", start_time=NOW - timedelta(seconds=started),
                           conditions=[V1PodCondition(type="Ready", status="True" if ready else "False")],
                           container_statuses=statuses),
    )


def test_no_annotation_means_not_opted_in():
    assert P.check_spec(pod()) is None


def test_check_spec_defaults_to_first_container_port():
    spec = P.check_spec(pod({"autohealer.io/check-path": "work"}))
    assert (spec.path, spec.port, spec.threshold, spec.timeout) == ("/work", 8080, None, 2.0)


def test_check_spec_overrides():
    spec = P.check_spec(pod({"autohealer.io/check-path": "/deep", "autohealer.io/check-port": "9000",
                             "autohealer.io/failure-threshold": "5", "autohealer.io/timeout-seconds": "0.5"}))
    assert (spec.port, spec.threshold, spec.timeout) == (9000, 5, 0.5)


def test_workload_groups_replicasets_under_their_deployment():
    assert P.workload_of(pod()) == "shop/Deployment/web"
    assert P.workload_of(pod(owner=("StatefulSet", "db"))) == "shop/StatefulSet/db"
    assert P.workload_of(pod(owner=None)) is None          # bare pods are never touched


def test_timing_helpers():
    assert P.running_for(pod(started=90), NOW) == 90
    assert P.terminating_for(pod(), NOW) is None
    assert P.terminating_for(pod(deletion_ago=700), NOW) == 700


def test_crashloop_detection_and_last_exit_reason():
    statuses = [V1ContainerStatus(
        name="app", image="x", image_id="x", ready=False, restart_count=7,
        state=V1ContainerState(waiting=V1ContainerStateWaiting(reason="CrashLoopBackOff")),
        last_state=V1ContainerState(terminated=V1ContainerStateTerminated(exit_code=137, reason="OOMKilled")))]
    p = pod(statuses=statuses)
    assert P.crashlooping(p, min_restarts=5) == ("app", 7)
    assert P.crashlooping(p, min_restarts=10) is None
    assert P.last_termination_reason(p, "app") == "OOMKilled (exit 137)"


def test_ready_condition():
    assert P.is_ready(pod(ready=True)) and not P.is_ready(pod(ready=False))
