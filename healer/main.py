"""The reconcile loop.

Every INTERVAL seconds (leader only):
1. List pods in the watched namespaces.
2. Functional checks: for opted-in pods that are Running and past the startup grace period,
   call http://<pod IP>:<port><path> in parallel. Failures go through the Policy; a REMEDIATE
   verdict evicts the pod (PDB-aware) so its controller replaces it.
3. Stuck Terminating: Deployment/ReplicaSet pods still terminating long after their grace
   period (typically on a node that stopped responding) are force-deleted. StatefulSet pods are
   never force-deleted: two pods with the same identity could end up running.
4. CrashLoopBackOff: reported (Event, metric, webhook), never restarted: a restart can't fix a
   crash loop, and evicting would only hide it.
5. Nodes (NODE_REMEDIATION=true): cordon and drain nodes NotReady or under pressure past a
   threshold, uncordon them on recovery; see nodes.py.

Two circuit breakers (breaker.py) stop actions when failures look systemic: "pods" opens when
more than POD_MAX_FAILING_FRACTION of the checked pods fail in one loop (a shared dependency is
down; evicting pods that are blocked on it would only add churn); "nodes" opens when more than
NODE_MAX_UNHEALTHY_FRACTION of worker nodes are unhealthy.

Environment (see chart/values.yaml): WATCH_NAMESPACES (comma list, empty = all), DRY_RUN,
INTERVAL_SECONDS, FAILURE_THRESHOLD, COOLDOWN_SECONDS, MAX_ACTIONS_PER_HOUR,
STARTUP_GRACE_SECONDS, STUCK_TERMINATING_SECONDS, CRASHLOOP_MIN_RESTARTS, NOTIFY_WEBHOOK_URL,
POD_NAMESPACE, POD_NAME (for leader election), POD_MAX_FAILING_FRACTION, NODE_REMEDIATION,
NODE_NOT_READY_SECONDS, NODE_PRESSURE_SECONDS, NODE_RECOVERY_SECONDS, NODE_MAX_UNHEALTHY_FRACTION,
NODE_MAX_ACTIONS_PER_HOUR, NODE_DRAIN.
"""
from __future__ import annotations

import logging
import os
import socket
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from kubernetes import client, config
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from . import pods as P
from .actions import Actions
from .breaker import CircuitBreaker
from .leader import LeaderElector
from .nodes import NodeRemediator
from .policy import ActionBudget, Policy, Verdict, WorkloadGuard

log = logging.getLogger("healer")

CHECKS = Counter("healer_checks_total", "Functional checks", ["result"])
REMEDIATIONS = Counter("healer_remediations_total", "Actions taken", ["reason", "result"])
SUPPRESSED = Counter("healer_suppressed_total", "Remediations held back by a safety rule", ["reason"])
CRASHLOOPS = Counter("healer_crashloops_detected_total", "Pods reported in CrashLoopBackOff")
LEADER = Gauge("healer_is_leader", "1 if this replica is the active leader")
CIRCUIT_OPEN = Gauge("healer_circuit_open", "1 while a circuit breaker is open (no actions in that scope)", ["scope"])
NODE_ACTIONS = Counter("healer_node_actions_total", "Node remediation steps", ["action"])
LOOP = Histogram("healer_loop_seconds", "Duration of one reconcile loop", buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def functional_check(pod, spec: P.CheckSpec) -> bool:
    url = f"http://{pod.status.pod_ip}:{spec.port}{spec.path}"
    try:
        with urllib.request.urlopen(url, timeout=spec.timeout) as r:
            return 200 <= r.status < 300
    except Exception:  # timeouts, connection errors, HTTP errors: all mean "not doing its job"
        return False


class Healer:
    def __init__(self):
        self.namespaces = [n.strip() for n in env("WATCH_NAMESPACES", "").split(",") if n.strip()]
        self.dry_run = env("DRY_RUN", "false").lower() == "true"
        self.interval = float(env("INTERVAL_SECONDS", "10"))
        self.startup_grace = float(env("STARTUP_GRACE_SECONDS", "60"))
        self.stuck_after = float(env("STUCK_TERMINATING_SECONDS", "600"))
        self.crashloop_restarts = int(env("CRASHLOOP_MIN_RESTARTS", "5"))
        self.policy = Policy(failure_threshold=int(env("FAILURE_THRESHOLD", "3")),
                             guard=WorkloadGuard(cooldown=float(env("COOLDOWN_SECONDS", "120"))),
                             budget=ActionBudget(limit=int(env("MAX_ACTIONS_PER_HOUR", "10"))))
        self.reported_crashloops: dict[str, float] = {}
        self.pod_breaker = CircuitBreaker("pods", reset_timeout=float(env("COOLDOWN_SECONDS", "120")))
        self.max_failing_fraction = float(env("POD_MAX_FAILING_FRACTION", "0.5"))
        self.pool = ThreadPoolExecutor(max_workers=32)

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.core = client.CoreV1Api()
        self.actions = Actions(self.core, self.dry_run, env("NOTIFY_WEBHOOK_URL", "") or None)
        self.nodes = None
        if env("NODE_REMEDIATION", "false").lower() == "true":
            self.nodes = NodeRemediator(
                list_nodes=lambda: self.core.list_node().items,
                pods_on=lambda name: self.core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={name}").items,
                actions=self.actions,
                not_ready_after=float(env("NODE_NOT_READY_SECONDS", "120")),
                pressure_after=float(env("NODE_PRESSURE_SECONDS", "120")),
                recover_after=float(env("NODE_RECOVERY_SECONDS", "120")),
                max_unhealthy_fraction=float(env("NODE_MAX_UNHEALTHY_FRACTION", "0.34")),
                drain=env("NODE_DRAIN", "true").lower() == "true",
                budget=ActionBudget(limit=int(env("NODE_MAX_ACTIONS_PER_HOUR", "2"))))
        self.elector = LeaderElector(client.CoordinationV1Api(), env("POD_NAMESPACE", "default"), "k8s-auto-healer",
                                     identity=env("POD_NAME", socket.gethostname()))

    def list_pods(self):
        if not self.namespaces:
            return self.core.list_pod_for_all_namespaces().items
        return [p for ns in self.namespaces for p in self.core.list_namespaced_pod(ns).items]

    def run_once(self) -> None:
        pod_list = self.list_pods()
        live = {p.metadata.uid for p in pod_list}
        self.policy.tracker.forget_missing(live)
        self.policy.guard.finish_gone(live)

        # 2. functional checks, in parallel
        candidates = []
        for pod in pod_list:
            spec = P.check_spec(pod)
            if (spec and pod.status.phase == "Running" and pod.status.pod_ip and P.terminating_for(pod) is None
                    and P.running_for(pod) >= self.startup_grace and P.workload_of(pod)):
                candidates.append((pod, spec))
        results = list(self.pool.map(lambda ps: functional_check(*ps), candidates))
        now = time.time()
        failing = results.count(False)
        if len(results) >= 4 and failing / len(results) > self.max_failing_fraction:
            reason = f"{failing} of {len(results)} checked pods failing at once"
            if self.pod_breaker.trip(now, reason):
                log.error("pod remediation PAUSED: %s (shared cause?)", reason)
                self.actions.notify(f"pod remediation PAUSED: {reason}. Likely a shared dependency; not evicting. Needs a human.")
        elif self.pod_breaker.clear_systemic(now, "failing fraction back under the limit"):
            log.warning("pod remediation resumed")
        CIRCUIT_OPEN.labels("pods").set(1 if self.pod_breaker.is_open else 0)
        for (pod, spec), healthy in zip(candidates, results):
            CHECKS.labels("pass" if healthy else "fail").inc()
            workload = P.workload_of(pod)
            verdict = self.policy.decide(pod.metadata.uid, workload, healthy, spec.threshold)
            if verdict == Verdict.REMEDIATE and not self.pod_breaker.allow(now):
                SUPPRESSED.labels("circuit_open").inc()
                log.info("held back %s/%s: circuit open (%s)", pod.metadata.namespace, pod.metadata.name, self.pod_breaker.reason)
            elif verdict == Verdict.REMEDIATE:
                self.remediate(pod, workload, spec)
            elif verdict != Verdict.NONE:
                SUPPRESSED.labels(verdict.value).inc()
                log.info("held back %s/%s: %s", pod.metadata.namespace, pod.metadata.name, verdict.value)

        # 3. stuck terminating
        for pod in pod_list:
            overdue = P.terminating_for(pod)
            if overdue is not None and overdue > self.stuck_after and P.owner_kind(pod) == "ReplicaSet":
                result = self.actions.force_delete(pod)
                REMEDIATIONS.labels("stuck_terminating", result).inc()
                msg = f"force-deleted after being stuck Terminating {int(overdue)}s past its grace period ({result})"
                self.actions.event(pod, "StuckTerminatingRemoved", msg)
                self.actions.notify(f"{pod.metadata.namespace}/{pod.metadata.name}: {msg}")

        # 4. crash loops: report once per pod per hour
        for pod in pod_list:
            found = P.crashlooping(pod, self.crashloop_restarts)
            if found and now - self.reported_crashloops.get(pod.metadata.uid, 0) > 3600:
                container, restarts = found
                reason = P.last_termination_reason(pod, container) or "unknown"
                msg = (f"container {container} in CrashLoopBackOff after {restarts} restarts, last exit: {reason}. "
                       "Not restarting: needs a fix (config, image, resources), not a reboot.")
                self.actions.event(pod, "CrashLoopDetected", msg)
                self.actions.notify(f"{pod.metadata.namespace}/{pod.metadata.name}: {msg}")
                CRASHLOOPS.inc()
                self.reported_crashloops[pod.metadata.uid] = now

        # 5. nodes
        if self.nodes:
            seen = len(self.nodes.log)
            self.nodes.step(now)
            for action, node, detail in self.nodes.log[seen:]:
                NODE_ACTIONS.labels(action).inc()
                log.warning("node %s: %s %s", node or "-", action, detail)
            del self.nodes.log[:-100]
            CIRCUIT_OPEN.labels("nodes").set(1 if self.nodes.breaker.is_open else 0)

    def remediate(self, pod, workload: str, spec: P.CheckSpec) -> None:
        try:
            result = self.actions.evict(pod)
        except Exception as e:  # noqa: BLE001 - API errors feed the breaker
            self.pod_breaker.record_failure(time.time(), f"evicting {pod.metadata.name} failed: {e}")
            raise
        self.pod_breaker.record_success(time.time())
        REMEDIATIONS.labels("functional_check_failed", result).inc()
        name = f"{pod.metadata.namespace}/{pod.metadata.name}"
        if result in ("evicted", "dry_run"):
            self.policy.acted(pod.metadata.uid, workload)
            msg = (f"failed functional check {spec.path} on port {spec.port} "
                   f"{self.policy.failure_threshold if spec.threshold is None else spec.threshold} times in a row; "
                   f"{'would evict (dry run)' if result == 'dry_run' else 'evicted so its controller replaces it'}")
            log.warning("%s: %s", name, msg)
            self.actions.event(pod, "FunctionalCheckFailed", msg)
            self.actions.notify(f"{name}: {msg}")
        else:
            SUPPRESSED.labels(result).inc()
            log.warning("%s: eviction not done: %s", name, result)

    def run(self) -> None:
        log.info("watching %s, dry_run=%s, interval=%ss", self.namespaces or "all namespaces", self.dry_run, self.interval)
        while True:
            started = time.monotonic()
            try:
                leader = self.elector.try_acquire_or_renew()
                LEADER.set(1 if leader else 0)
                if leader:
                    with LOOP.time():
                        self.run_once()
            except Exception:  # keep the controller alive; the next loop retries
                log.exception("reconcile loop failed")
            time.sleep(max(0.0, self.interval - (time.monotonic() - started)))


def main() -> None:
    logging.basicConfig(level=env("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    start_http_server(int(env("METRICS_PORT", "9100")))
    Healer().run()


if __name__ == "__main__":
    main()
