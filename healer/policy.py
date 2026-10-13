"""Decision logic, kept free of Kubernetes calls so it can be unit-tested exhaustively.

The healer only acts when ALL of these hold:
1. The pod has failed its functional check N times in a row (one failure is noise).
2. No other pod of the same workload is being remediated or was remediated within the cooldown
   (never take down several replicas of one service at once; give the replacement time to start).
3. The global action budget for the last hour isn't spent (a systemic problem, like a dead
   database making every pod fail, must page a human instead of churning the whole cluster).
Anything blocked is counted and reported, so suppressed actions are visible, not silent.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum


class Verdict(str, Enum):
    NONE = "none"                      # nothing to do
    REMEDIATE = "remediate"
    SUPPRESSED_COOLDOWN = "suppressed_cooldown"
    SUPPRESSED_BUDGET = "suppressed_budget"
    SUPPRESSED_IN_PROGRESS = "suppressed_in_progress"


@dataclass
class FailureTracker:
    """Consecutive functional-check failures per pod."""
    counts: dict[str, int] = field(default_factory=dict)

    def record(self, pod_uid: str, healthy: bool) -> int:
        self.counts[pod_uid] = 0 if healthy else self.counts.get(pod_uid, 0) + 1
        return self.counts[pod_uid]

    def forget_missing(self, live_uids: set[str]) -> None:
        for uid in list(self.counts):
            if uid not in live_uids:
                del self.counts[uid]


@dataclass
class ActionBudget:
    """At most `limit` actions in any sliding `window` seconds."""
    limit: int
    window: float = 3600.0
    times: deque = field(default_factory=deque)

    def _trim(self, now: float) -> None:
        while self.times and now - self.times[0] > self.window:
            self.times.popleft()

    def available(self, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        self._trim(now)
        return len(self.times) < self.limit

    def spend(self, now: float | None = None) -> None:
        self.times.append(time.time() if now is None else now)


@dataclass
class WorkloadGuard:
    """One remediation per workload at a time, then a cooldown before the next."""
    cooldown: float
    last_action: dict[str, float] = field(default_factory=dict)
    in_progress: dict[str, str] = field(default_factory=dict)   # workload -> pod uid being replaced

    def blocked_by(self, workload: str, now: float) -> Verdict | None:
        if workload in self.in_progress:
            return Verdict.SUPPRESSED_IN_PROGRESS
        if now - self.last_action.get(workload, float("-inf")) < self.cooldown:
            return Verdict.SUPPRESSED_COOLDOWN
        return None

    def start(self, workload: str, pod_uid: str, now: float) -> None:
        self.in_progress[workload] = pod_uid
        self.last_action[workload] = now

    def finish_gone(self, live_uids: set[str]) -> None:
        """A remediation is finished once the evicted pod no longer exists."""
        for workload, uid in list(self.in_progress.items()):
            if uid not in live_uids:
                del self.in_progress[workload]


@dataclass
class Policy:
    failure_threshold: int
    guard: WorkloadGuard
    budget: ActionBudget
    tracker: FailureTracker = field(default_factory=FailureTracker)

    def decide(self, pod_uid: str, workload: str, healthy: bool, threshold: int | None = None,
               now: float | None = None) -> Verdict:
        now = time.time() if now is None else now
        failures = self.tracker.record(pod_uid, healthy)
        if failures < (threshold or self.failure_threshold):
            return Verdict.NONE
        blocked = self.guard.blocked_by(workload, now)
        if blocked:
            return blocked
        if not self.budget.available(now):
            return Verdict.SUPPRESSED_BUDGET
        return Verdict.REMEDIATE

    def acted(self, pod_uid: str, workload: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self.guard.start(workload, pod_uid, now)
        self.budget.spend(now)
        self.tracker.counts.pop(pod_uid, None)
