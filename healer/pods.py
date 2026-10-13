"""Reading what the healer needs out of a Pod object (pure functions, unit-tested)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

ANNOTATION_PREFIX = "autohealer.io/"


@dataclass
class CheckSpec:
    path: str
    port: int
    threshold: int | None
    timeout: float


def check_spec(pod) -> CheckSpec | None:
    """The functional check a pod opted into, or None if it didn't opt in.

    Opt-in is per pod template, through annotations, so teams choose what the healer may touch:
      autohealer.io/check-path: /work          (required: a path that exercises real work)
      autohealer.io/check-port: "8080"         (default: the first container port)
      autohealer.io/failure-threshold: "3"     (default: the healer's global setting)
      autohealer.io/timeout-seconds: "2"
    """
    ann = pod.metadata.annotations or {}
    path = ann.get(ANNOTATION_PREFIX + "check-path")
    if not path:
        return None
    port = ann.get(ANNOTATION_PREFIX + "check-port")
    if port is None:
        ports = [p.container_port for c in pod.spec.containers for p in (c.ports or [])]
        if not ports:
            return None
        port = ports[0]
    threshold = ann.get(ANNOTATION_PREFIX + "failure-threshold")
    return CheckSpec(path=path if path.startswith("/") else "/" + path, port=int(port),
                     threshold=int(threshold) if threshold else None,
                     timeout=float(ann.get(ANNOTATION_PREFIX + "timeout-seconds", "2")))


def workload_of(pod) -> str | None:
    """namespace/kind/name of the controller owning the pod, or None for bare pods.

    Deployment pods are owned by a ReplicaSet named <deployment>-<pod-template-hash>; grouping by
    the Deployment name keeps "one remediation per workload" true across rollouts.
    """
    for ref in pod.metadata.owner_references or []:
        if not ref.controller:
            continue
        if ref.kind == "ReplicaSet":
            return f"{pod.metadata.namespace}/Deployment/{ref.name.rsplit('-', 1)[0]}"
        return f"{pod.metadata.namespace}/{ref.kind}/{ref.name}"
    return None


def owner_kind(pod) -> str | None:
    for ref in pod.metadata.owner_references or []:
        if ref.controller:
            return ref.kind
    return None


def is_ready(pod) -> bool:
    return any(c.type == "Ready" and c.status == "True" for c in (pod.status.conditions or []))


def running_for(pod, now: datetime | None = None) -> float:
    """Seconds since the pod started (0 if not started)."""
    if not pod.status.start_time:
        return 0.0
    now = now or datetime.now(timezone.utc)
    return (now - pod.status.start_time).total_seconds()


def terminating_for(pod, now: datetime | None = None) -> float | None:
    """Seconds past the end of the pod's grace period, or None if it isn't terminating."""
    ts = pod.metadata.deletion_timestamp
    if ts is None:
        return None
    now = now or datetime.now(timezone.utc)
    return (now - ts).total_seconds()


def crashlooping(pod, min_restarts: int) -> tuple[str, int] | None:
    """(container name, restarts) of a container in CrashLoopBackOff with enough restarts."""
    for cs in pod.status.container_statuses or []:
        waiting = cs.state.waiting if cs.state else None
        if waiting and waiting.reason == "CrashLoopBackOff" and cs.restart_count >= min_restarts:
            return cs.name, cs.restart_count
    return None


def last_termination_reason(pod, container: str) -> str | None:
    for cs in pod.status.container_statuses or []:
        if cs.name == container and cs.last_state and cs.last_state.terminated:
            t = cs.last_state.terminated
            return f"{t.reason or 'Error'} (exit {t.exit_code})"
    return None
