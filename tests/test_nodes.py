from datetime import datetime, timedelta, timezone

from kubernetes.client import (V1Node, V1NodeCondition, V1NodeSpec, V1NodeStatus, V1ObjectMeta, V1OwnerReference,
                               V1Pod, V1PodStatus)

from healer import nodes as N
from healer.policy import ActionBudget

T0 = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
NOW = T0.timestamp()


def node(name, ready="True", since=3600, pressure=None, cordoned=False, by=None, labels=None, annotations=None):
    conds = [V1NodeCondition(type="Ready", status=ready, last_transition_time=T0 - timedelta(seconds=since))]
    for t in N.PRESSURE:
        status = "True" if pressure == t else "False"
        conds.append(V1NodeCondition(type=t, status=status, last_transition_time=T0 - timedelta(seconds=since)))
    ann = dict(annotations or {})
    if by:
        ann[N.CORDONED_BY] = by
    return V1Node(metadata=V1ObjectMeta(name=name, uid=f"uid-{name}", labels=labels or {}, annotations=ann),
                  spec=V1NodeSpec(unschedulable=cordoned), status=V1NodeStatus(conditions=conds))


def pod(name, owner="ReplicaSet", phase="Running", mirror=False):
    return V1Pod(metadata=V1ObjectMeta(name=name, namespace="shop",
                                       annotations={"kubernetes.io/config.mirror": "x"} if mirror else None,
                                       owner_references=[V1OwnerReference(api_version="v1", kind=owner, name="o", uid="u")]),
                 status=V1PodStatus(phase=phase))


class FakeActions:
    def __init__(self, evict_result="evicted"):
        self.calls, self.evict_result = [], evict_result

    def cordon(self, n, identity, reason):
        self.calls.append(("cordon", n.metadata.name, reason))
        n.spec.unschedulable = True
        n.metadata.annotations[N.CORDONED_BY] = identity

    def uncordon(self, n):
        self.calls.append(("uncordon", n.metadata.name))

    def evict(self, p):
        self.calls.append(("evict", p.metadata.name))
        return self.evict_result

    def node_event(self, n, reason, message):
        self.calls.append(("event", n.metadata.name, reason))

    def notify(self, text):
        self.calls.append(("notify", text))


def remediator(cluster, pods=None, actions=None, **kw):
    return N.NodeRemediator(list_nodes=lambda: cluster, pods_on=lambda name: (pods or {}).get(name, []),
                            actions=actions or FakeActions(), **kw)


def calls(r, kind):
    return [c for c in r.actions.calls if c[0] == kind]


# --- condition parsing -----------------------------------------------------------------------

def test_problem_detection():
    assert N.problem(node("a"), NOW) is None
    assert N.problem(node("a", ready="Unknown", since=200), NOW) == ("NotReady", 200)
    assert N.problem(node("a", pressure="DiskPressure", since=90), NOW) == ("DiskPressure", 90)


def test_drainable_skips_daemonsets_static_and_finished_pods():
    assert N.drainable(pod("web"))
    assert not N.drainable(pod("fluentbit", owner="DaemonSet"))
    assert not N.drainable(pod("kube-proxy", mirror=True))
    assert not N.drainable(pod("job", phase="Succeeded"))


# --- remediation -----------------------------------------------------------------------------

def test_not_ready_past_threshold_is_cordoned_and_drained():
    cluster = [node("w1", ready="Unknown", since=180), node("w2"), node("w3")]
    r = remediator(cluster, pods={"w1": [pod("web-1"), pod("fluentbit", owner="DaemonSet")]})
    r.step(NOW)
    assert calls(r, "cordon") == [("cordon", "w1", "NotReady for 180s")]
    assert calls(r, "evict") == [("evict", "web-1")]           # DaemonSet pod left alone


def test_below_threshold_nothing_happens():
    r = remediator([node("w1", ready="False", since=60), node("w2"), node("w3")])
    r.step(NOW)
    assert r.actions.calls == []


def test_control_plane_and_ignored_nodes_are_never_touched():
    cluster = [node("cp", ready="False", since=999, labels={"node-role.kubernetes.io/control-plane": ""}),
               node("w1", ready="False", since=999, annotations={N.IGNORE: "true"}), node("w2"), node("w3"), node("w4")]
    r = remediator(cluster)
    r.step(NOW)
    assert calls(r, "cordon") == []


def test_one_node_at_a_time_until_drained():
    cluster = [node(f"w{i}") for i in range(1, 7)]
    cluster[0] = node("w1", ready="False", since=500)
    cluster[1] = node("w2", pressure="MemoryPressure", since=400)
    r = remediator(cluster, pods={"w1": [pod("web-1")]}, actions=FakeActions(evict_result="blocked_by_pdb"))
    r.step(NOW)
    r.step(NOW + 10)                                 # w1 still has a PDB-blocked pod: keep draining w1
    assert [c[1] for c in calls(r, "cordon")] == ["w1"]
    assert len(calls(r, "evict")) == 2


def test_circuit_breaker_opens_when_too_many_nodes_unhealthy():
    cluster = [node("w1", ready="Unknown", since=900), node("w2", ready="Unknown", since=900), node("w3")]
    r = remediator(cluster)
    r.step(NOW)
    r.step(NOW + 10)
    assert calls(r, "cordon") == [] and r.breaker.is_open
    assert len(calls(r, "notify")) == 1 and "PAUSED: 2 of 3 worker nodes unhealthy" in calls(r, "notify")[0][1]
    # one recovers: under the limit again -> breaker closes and the remaining bad node is handled
    cluster[1] = node("w2")
    r.step(NOW + 20)
    assert not r.breaker.is_open and [c[1] for c in calls(r, "cordon")] == ["w1"]


def test_node_budget():
    cluster = [node(f"w{i}") for i in range(1, 11)]
    r = remediator(cluster, drain=False, budget=ActionBudget(limit=1))
    cluster[0] = node("w1", ready="False", since=500)
    cluster[1] = node("w2", ready="False", since=400)
    r.list_nodes = lambda: cluster
    r.step(NOW)
    r.step(NOW + 10)
    assert [c[1] for c in calls(r, "cordon")] == ["w1"] and r.log[-1][0] == "suppressed_budget"


def test_uncordons_only_nodes_it_cordoned_after_recovery():
    cluster = [node("w1", cordoned=True, by="k8s-auto-healer", since=300),
               node("w2", cordoned=True, since=3000),                    # cordoned by an admin
               node("w3", cordoned=True, by="k8s-auto-healer", since=30),  # not healthy long enough
               node("w4"), node("w5")]
    r = remediator(cluster)
    r.step(NOW)
    assert calls(r, "uncordon") == [("uncordon", "w1")]


def test_cordon_failures_feed_the_breaker():
    class Failing(FakeActions):
        def cordon(self, n, identity, reason):
            raise RuntimeError("apiserver timeout")

    cluster = [node("w1", ready="False", since=500)] + [node(f"w{i}") for i in range(2, 6)]
    r = remediator(cluster, actions=Failing())
    for i in range(3):
        r.step(NOW + i)
    assert r.breaker.is_open and "cordon w1 failed" in r.breaker.reason
