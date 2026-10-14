"""Circuit breaker for automated remediation.

Automation that keeps acting while the world is on fire makes outages worse: if half the pods
fail their checks at once, or a third of the nodes go NotReady together, the cause is shared
(a database, the network, the control plane) and evicting or draining only adds churn. The
breaker stops all actions in its scope until things look normal again, and says so loudly.

    CLOSED     actions allowed
    OPEN       no actions; entered when the systemic signal fires (trip) or after
               `failure_threshold` consecutive failed actions
    HALF_OPEN  after `reset_timeout`, exactly one trial action is allowed: success closes the
               breaker, failure re-opens it

Pure logic (time is passed in), so every transition is unit-tested.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class State(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    scope: str
    failure_threshold: int = 3
    reset_timeout: float = 600.0
    state: State = State.CLOSED
    failures: int = 0
    opened_at: float = 0.0
    reason: str = ""
    trial_in_flight: bool = False
    systemic: bool = False                            # opened by trip() rather than by failures
    transitions: list = field(default_factory=list)   # (time, state, reason): for logs/tests

    def _set(self, state: State, now: float, reason: str = "") -> bool:
        changed = state != self.state
        self.state = state
        if state == State.OPEN:
            self.opened_at, self.reason, self.trial_in_flight = now, reason, False
        if changed:
            self.transitions.append((now, state, reason))
        return changed

    def trip(self, now: float, reason: str) -> bool:
        """Systemic signal (e.g. too many unhealthy nodes). Re-tripping refreshes the timer.
        Returns True when this call opened a closed/half-open breaker (worth an alert)."""
        opened = self._set(State.OPEN, now, reason)
        self.systemic = True
        return opened

    def clear_systemic(self, now: float, reason: str) -> bool:
        """The systemic signal is gone: close a breaker that trip() opened, without waiting for
        the reset timeout (a breaker opened by failed actions keeps its normal half-open path)."""
        if self.state == State.OPEN and self.systemic:
            self.systemic = False
            return self._set(State.CLOSED, now, reason)
        return False

    def allow(self, now: float) -> bool:
        if self.state == State.CLOSED:
            return True
        if self.state == State.OPEN and now - self.opened_at >= self.reset_timeout:
            self._set(State.HALF_OPEN, now, "reset timeout elapsed: one trial action")
        if self.state == State.HALF_OPEN and not self.trial_in_flight:
            self.trial_in_flight = True
            return True
        return False

    def record_success(self, now: float) -> None:
        self.failures = 0
        self.trial_in_flight = False
        if self.state == State.HALF_OPEN:
            self._set(State.CLOSED, now, "trial action succeeded")

    def record_failure(self, now: float, reason: str = "action failed") -> None:
        self.failures += 1
        self.trial_in_flight = False
        if self.state == State.HALF_OPEN or self.failures >= self.failure_threshold:
            self._set(State.OPEN, now, f"{reason} ({self.failures} in a row)")
            self.systemic = False

    @property
    def is_open(self) -> bool:
        return self.state != State.CLOSED
