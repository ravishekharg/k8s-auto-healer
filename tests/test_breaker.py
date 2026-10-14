from healer.breaker import CircuitBreaker, State


def test_closed_allows_and_failures_open_it():
    b = CircuitBreaker("pods", failure_threshold=3, reset_timeout=60)
    assert b.allow(0)
    b.record_failure(1)
    b.record_failure(2)
    assert b.state == State.CLOSED
    b.record_failure(3)
    assert b.state == State.OPEN and not b.allow(4)


def test_success_resets_the_failure_count():
    b = CircuitBreaker("pods", failure_threshold=2)
    b.record_failure(1)
    b.record_success(2)
    b.record_failure(3)
    assert b.state == State.CLOSED


def test_half_open_allows_exactly_one_trial():
    b = CircuitBreaker("nodes", reset_timeout=60)
    b.trip(0, "3 of 4 nodes unhealthy")
    assert not b.allow(30)
    assert b.allow(61) and b.state == State.HALF_OPEN
    assert not b.allow(62)            # second action while the trial is in flight


def test_trial_success_closes_failure_reopens():
    b = CircuitBreaker("nodes", reset_timeout=60)
    b.trip(0, "x")
    b.allow(61)
    b.record_success(62)
    assert b.state == State.CLOSED
    b.trip(100, "x")
    b.allow(161)
    b.record_failure(162)
    assert b.state == State.OPEN and b.opened_at == 162


def test_retrip_refreshes_timer_and_reports_only_the_first_opening():
    b = CircuitBreaker("nodes", reset_timeout=60)
    assert b.trip(0, "x") is True
    assert b.trip(50, "x") is False   # still open: no second alert
    assert not b.allow(70)            # timer restarted at 50


def test_clear_systemic_closes_only_breakers_opened_by_trip():
    b = CircuitBreaker("nodes", failure_threshold=1)
    b.trip(0, "too many unhealthy")
    assert b.clear_systemic(10, "back to normal") and b.state == State.CLOSED
    b.record_failure(20)             # opened by a failed action, not by the systemic signal
    assert not b.clear_systemic(30, "x") and b.state == State.OPEN
