"""Leader election on a coordination.k8s.io Lease, so the healer can run with several replicas
and exactly one of them acts.

Algorithm (the same idea client-go's leaderelection package uses):
- Read the Lease. If nobody holds it, or the holder hasn't renewed within leaseDurationSeconds,
  take it by writing our identity, with the resourceVersion we read. If another replica wrote in
  between, the API server rejects our write with 409 Conflict and we simply aren't leader.
- The leader renews (rewrites renewTime) every loop. If it stops (crash, network partition),
  another replica takes over after the lease expires.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.rest import ApiException

log = logging.getLogger("healer.leader")


class LeaderElector:
    def __init__(self, api: client.CoordinationV1Api, namespace: str, name: str, identity: str, lease_seconds: int = 30):
        self.api, self.namespace, self.name = api, namespace, name
        self.identity, self.lease_seconds = identity, lease_seconds
        self.is_leader = False

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _spec(self, acquire_time=None, transitions=0) -> client.V1LeaseSpec:
        now = self._now()
        return client.V1LeaseSpec(holder_identity=self.identity, lease_duration_seconds=self.lease_seconds,
                                  acquire_time=acquire_time or now, renew_time=now, lease_transitions=transitions)

    def try_acquire_or_renew(self) -> bool:
        try:
            lease = self.api.read_namespaced_lease(self.name, self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise
            body = client.V1Lease(metadata=client.V1ObjectMeta(name=self.name, namespace=self.namespace), spec=self._spec())
            try:
                self.api.create_namespaced_lease(self.namespace, body)
                return self._set(True)
            except ApiException as e2:
                if e2.status == 409:   # another replica created it first
                    return self._set(False)
                raise

        spec = lease.spec
        holder = spec.holder_identity
        expired = (spec.renew_time is None or
                   (self._now() - spec.renew_time).total_seconds() > (spec.lease_duration_seconds or self.lease_seconds))
        if holder not in (None, "", self.identity) and not expired:
            return self._set(False)

        transitions = (spec.lease_transitions or 0) + (0 if holder == self.identity else 1)
        acquire = spec.acquire_time if holder == self.identity else None
        lease.spec = self._spec(acquire_time=acquire, transitions=transitions)
        try:
            self.api.replace_namespaced_lease(self.name, self.namespace, lease)   # carries resourceVersion
            return self._set(True)
        except ApiException as e:
            if e.status == 409:
                return self._set(False)
            raise

    def _set(self, leader: bool) -> bool:
        if leader != self.is_leader:
            log.info("%s leadership: %s", "acquired" if leader else "lost/not holding", self.identity)
        self.is_leader = leader
        return leader
