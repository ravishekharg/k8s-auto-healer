"""Node remediation: cordon and drain nodes that are unhealthy for longer than a threshold,
uncordon them when they recover, and stop everything when too many are unhealthy at once.

What counts as unhealthy (from the Node's conditions, as reported by the kubelet / node
controller):
  NotReady   Ready is False or Unknown (kubelet stopped reporting, runtime or network broken)
  pressure   MemoryPressure, DiskPressure or PIDPressure is True

What Kubernetes already does, and what this adds:
  - For NotReady, the node controller taints the node and evicts its pods after the pods'
    toleration (300 s by default). Pods then sit in Terminating as long as the kubelet is gone.
    Cordoning stops new pods landing there if the node flaps back; draining starts the move
    without waiting for the toleration; the pod loop's stuck-Terminating handling then frees
    ReplicaSet replicas so they're rescheduled elsewhere.
  - For pressure, the kubelet starts evicting pods itself, abruptly and without regard to
    PodDisruptionBudgets. Draining first moves workloads through the Eviction API, which
    honours PDBs.

Safety:
  - one node at a time, and a separate hourly budget for node actions
  - circuit breaker: if more than `max_unhealthy_fraction` of worker nodes are unhealthy, the
    problem is shared (network, control plane, a bad node image rollout): no node actions
  - control-plane nodes and nodes annotated autohealer.io/ignore=true are never touched
  - only nodes this healer cordoned (annotation) are ever uncordoned; a node an admin cordoned
    stays cordoned
  - DaemonSet pods and static (mirror) pods are not evicted, as with `kubectl drain`
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from .breaker import CircuitBreaker
from .policy import ActionBudget

CORDONED_BY = "autohealer.io/cordoned-by"
CORDON_REASON = "autohealer.io/cordon-reason"
IGNORE = "autohealer.io/ignore"
PRESSURE = ("MemoryPressure", "DiskPressure", "PIDPressure")


def _age(ts, now: float) -> float:
    if ts is None:
        return 0.0
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return now - ts.timestamp()


def conditions(node) -> dict:
    return {c.type: c for c in (node.status.conditions or [])}


def problem(node, now: float):
    """(reason, seconds it has lasted), or None if the node is healthy."""
    cond = conditions(node)
    ready = cond.get("Ready")
    if ready is None or ready.status != "True":
        return "NotReady", _age(ready.last_transition_time if ready else None, now)
    for t in PRESSURE:
        c = cond.get(t)
        if c is not None and c.status == "True":
            return t, _age(c.last_transition_time, now)
    return None


def healthy_for(node, now: float) -> float:
    """Seconds since the most recent of: Ready became True, any pressure condition cleared."""
    cond = conditions(node)
    times = [cond[t].last_transition_time for t in ("Ready", *PRESSURE) if t in cond]
    return min((_age(t, now) for t in times), default=0.0)


def annotations(node) -> dict:
    return node.metadata.annotations or {}


def is_control_plane(node) -> bool:
    labels = node.metadata.labels or {}
    return "node-role.kubernetes.io/control-plane" in labels or "node-role.kubernetes.io/master" in labels


def managed(node) -> bool:
    return not is_control_plane(node) and annotations(node).get(IGNORE, "").lower() != "true"


def cordoned_by_us(node, identity: str) -> bool:
    return bool(node.spec.unschedulable) and annotations(node).get(CORDONED_BY) == identity


def drainable(pod) -> bool:
    """Pods `kubectl drain` would evict: not DaemonSet-managed, not static, not finished."""
    if pod.status.phase in ("Succeeded", "Failed") or pod.metadata.deletion_timestamp:
        return False
    if (pod.metadata.annotations or {}).get("kubernetes.io/config.mirror"):
        return False
    return not any(o.kind == "DaemonSet" for o in (pod.metadata.owner_references or []))


@dataclass
class NodeRemediator:
    """One reconcile step per call. Kubernetes access is injected so it can be tested with fakes.

    list_nodes()            -> nodes
    pods_on(node_name)      -> pods scheduled on that node (all namespaces)
    actions                 -> .cordon(node, identity, reason) / .uncordon(node) / .evict(pod) /
                               .node_event(node, reason, message) / .notify(text)
    """
    list_nodes: Callable
    pods_on: Callable
    actions: object
    identity: str = "k8s-auto-healer"
    not_ready_after: float = 120.0
    pressure_after: float = 120.0
    recover_after: float = 120.0
    max_unhealthy_fraction: float = 0.34
    drain: bool = True
    budget: ActionBudget = field(default_factory=lambda: ActionBudget(limit=2))
    breaker: CircuitBreaker = field(default_factory=lambda: CircuitBreaker("nodes", reset_timeout=900))
    in_progress: str | None = None          # node being drained
    log: list = field(default_factory=list)  # (event, node, detail): metrics/tests

    def step(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        nodes = [n for n in self.list_nodes() if managed(n)]
        if not nodes:
            return
        unhealthy = {n.metadata.name: problem(n, now) for n in nodes if problem(n, now)}

        # Circuit breaker: a shared cause, not a bad node.
        fraction = len(unhealthy) / len(nodes)
        if fraction > self.max_unhealthy_fraction:
            reason = f"{len(unhealthy)} of {len(nodes)} worker nodes unhealthy ({', '.join(sorted(unhealthy))})"
            if self.breaker.trip(now, reason):
                self.log.append(("breaker_open", None, reason))
                self.actions.notify(f"node remediation PAUSED: {reason}. Likely a shared cause "
                                    "(network, control plane, node image); not draining anything. Needs a human.")
            return
        if self.breaker.clear_systemic(now, "unhealthy fraction back under the limit"):
            self.log.append(("breaker_closed", None, "unhealthy fraction back under the limit"))
            self.actions.notify("node remediation resumed: unhealthy nodes back under the limit")

        # Finish (keep draining) the node in progress before starting another.
        if self.in_progress:
            node = next((n for n in nodes if n.metadata.name == self.in_progress), None)
            if node is None or not cordoned_by_us(node, self.identity):
                self.in_progress = None
            elif self.drain:
                if self._drain(node) == 0:
                    self.log.append(("drained", node.metadata.name, ""))
                    self.in_progress = None
                return

        # Uncordon nodes we cordoned once they've been healthy long enough.
        for node in nodes:
            name = node.metadata.name
            if cordoned_by_us(node, self.identity) and name not in unhealthy and healthy_for(node, now) >= self.recover_after:
                self.actions.uncordon(node)
                self.actions.node_event(node, "NodeRecovered", f"healthy for {int(healthy_for(node, now))}s: uncordoned")
                self.log.append(("uncordoned", name, ""))

        # Start on the unhealthy node that has been unhealthy longest.
        if self.in_progress:
            return
        due = []
        for node in nodes:
            p = unhealthy.get(node.metadata.name)
            if p and not node.spec.unschedulable:
                reason, lasted = p
                if lasted >= (self.not_ready_after if reason == "NotReady" else self.pressure_after):
                    due.append((lasted, node, reason))
        if not due:
            return
        lasted, node, reason = max(due, key=lambda d: d[0])
        name = node.metadata.name
        if not self.breaker.allow(now):
            self.log.append(("suppressed_breaker", name, self.breaker.reason))
            return
        if not self.budget.available(now):
            self.log.append(("suppressed_budget", name, ""))
            return
        try:
            self.actions.cordon(node, self.identity, f"{reason} for {int(lasted)}s")
        except Exception as e:  # noqa: BLE001 - feed the breaker, try again next loop
            self.breaker.record_failure(now, f"cordon {name} failed: {e}")
            self.log.append(("cordon_failed", name, str(e)))
            return
        self.budget.spend(now)
        self.breaker.record_success(now)
        msg = f"{reason} for {int(lasted)}s: cordoned" + (" and draining" if self.drain else "")
        self.actions.node_event(node, "NodeCordoned", msg)
        self.actions.notify(f"node {name}: {msg}")
        self.log.append(("cordoned", name, reason))
        if self.drain:
            self.in_progress = name
            self._drain(node)

    def _drain(self, node) -> int:
        """Evict what can be evicted now; returns how many drainable pods remain."""
        remaining = 0
        for pod in self.pods_on(node.metadata.name):
            if not drainable(pod):
                continue
            result = self.actions.evict(pod)
            if result not in ("evicted", "dry_run", "already_gone"):
                remaining += 1          # e.g. blocked_by_pdb: try again next loop
        return remaining
