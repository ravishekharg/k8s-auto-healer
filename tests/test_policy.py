from healer.policy import ActionBudget, FailureTracker, Policy, Verdict, WorkloadGuard


def policy(threshold=3, cooldown=60, limit=5):
    return Policy(failure_threshold=threshold, guard=WorkloadGuard(cooldown=cooldown), budget=ActionBudget(limit=limit))


def test_needs_consecutive_failures_before_acting():
    p = policy(threshold=3)
    assert p.decide("pod-a", "web", healthy=False, now=0) == Verdict.NONE
    assert p.decide("pod-a", "web", healthy=False, now=1) == Verdict.NONE
    assert p.decide("pod-a", "web", healthy=False, now=2) == Verdict.REMEDIATE


def test_a_healthy_check_resets_the_count():
    p = policy(threshold=2)
    p.decide("pod-a", "web", healthy=False, now=0)
    p.decide("pod-a", "web", healthy=True, now=1)
    assert p.decide("pod-a", "web", healthy=False, now=2) == Verdict.NONE


def test_per_pod_threshold_override():
    p = policy(threshold=5)
    assert p.decide("pod-a", "web", healthy=False, threshold=1, now=0) == Verdict.REMEDIATE


def test_only_one_pod_per_workload_at_a_time():
    p = policy(threshold=1)
    assert p.decide("pod-a", "web", healthy=False, now=0) == Verdict.REMEDIATE
    p.acted("pod-a", "web", now=0)
    # A second replica of the same workload fails while the first is still being replaced.
    assert p.decide("pod-b", "web", healthy=False, now=1) == Verdict.SUPPRESSED_IN_PROGRESS
    # Another workload is unaffected.
    assert p.decide("pod-x", "api", healthy=False, now=1) == Verdict.REMEDIATE


def test_cooldown_after_the_replaced_pod_is_gone():
    p = policy(threshold=1, cooldown=60)
    p.acted("pod-a", "web", now=0)
    p.guard.finish_gone(live_uids={"pod-b"})          # pod-a has been replaced
    assert p.decide("pod-b", "web", healthy=False, now=30) == Verdict.SUPPRESSED_COOLDOWN
    assert p.decide("pod-b", "web", healthy=False, now=61) == Verdict.REMEDIATE


def test_global_budget_stops_cluster_wide_churn():
    p = policy(threshold=1, cooldown=0, limit=2)
    for i, wl in enumerate(["a", "b"]):
        assert p.decide(f"pod-{wl}", wl, healthy=False, now=i) == Verdict.REMEDIATE
        p.acted(f"pod-{wl}", wl, now=i)
    assert p.decide("pod-c", "c", healthy=False, now=5) == Verdict.SUPPRESSED_BUDGET
    # The window slides: an hour later there is budget again.
    assert p.decide("pod-c", "c", healthy=False, now=3700) == Verdict.REMEDIATE


def test_tracker_forgets_deleted_pods():
    t = FailureTracker()
    t.record("old", healthy=False)
    t.forget_missing({"new"})
    assert "old" not in t.counts
