from datetime import datetime, timedelta, timezone

import copy

from kubernetes.client import V1Lease, V1LeaseSpec, V1ObjectMeta
from kubernetes.client.rest import ApiException

from healer.leader import LeaderElector


class FakeLeaseApi:
    """In-memory Lease API with resourceVersion checks, like the real API server."""

    def __init__(self):
        self.lease = None
        self.version = 0
        self.conflict_next_replace = False

    def read_namespaced_lease(self, name, namespace):
        if self.lease is None:
            raise ApiException(status=404)
        return copy.deepcopy(self.lease)

    def create_namespaced_lease(self, namespace, body):
        if self.lease is not None:
            raise ApiException(status=409)
        self.version += 1
        body.metadata.resource_version = str(self.version)
        self.lease = copy.deepcopy(body)
        return body

    def replace_namespaced_lease(self, name, namespace, body):
        if self.conflict_next_replace or body.metadata.resource_version != self.lease.metadata.resource_version:
            self.conflict_next_replace = False
            raise ApiException(status=409)
        self.version += 1
        body.metadata.resource_version = str(self.version)
        self.lease = copy.deepcopy(body)
        return body


def held_by(api, holder, renewed_seconds_ago, duration=30):
    api.version += 1
    api.lease = V1Lease(metadata=V1ObjectMeta(name="k8s-auto-healer", resource_version=str(api.version)),
                        spec=V1LeaseSpec(holder_identity=holder, lease_duration_seconds=duration,
                                         renew_time=datetime.now(timezone.utc) - timedelta(seconds=renewed_seconds_ago),
                                         lease_transitions=0))


def test_first_replica_creates_the_lease_and_leads():
    api = FakeLeaseApi()
    assert LeaderElector(api, "ns", "k8s-auto-healer", "a").try_acquire_or_renew()
    assert api.lease.spec.holder_identity == "a"


def test_second_replica_does_not_lead_while_lease_is_fresh():
    api = FakeLeaseApi()
    held_by(api, "a", renewed_seconds_ago=5)
    assert not LeaderElector(api, "ns", "k8s-auto-healer", "b").try_acquire_or_renew()


def test_takes_over_an_expired_lease_and_counts_the_transition():
    api = FakeLeaseApi()
    held_by(api, "a", renewed_seconds_ago=60)
    assert LeaderElector(api, "ns", "k8s-auto-healer", "b").try_acquire_or_renew()
    assert api.lease.spec.holder_identity == "b" and api.lease.spec.lease_transitions == 1


def test_leader_renews_its_own_lease():
    api = FakeLeaseApi()
    a = LeaderElector(api, "ns", "k8s-auto-healer", "a")
    a.try_acquire_or_renew()
    first = api.lease.spec.renew_time
    assert a.try_acquire_or_renew()
    assert api.lease.spec.renew_time >= first and api.lease.spec.lease_transitions == 0


def test_losing_a_write_race_means_not_leader():
    api = FakeLeaseApi()
    held_by(api, "a", renewed_seconds_ago=60)
    api.conflict_next_replace = True        # another replica wrote between our read and write
    assert not LeaderElector(api, "ns", "k8s-auto-healer", "b").try_acquire_or_renew()
