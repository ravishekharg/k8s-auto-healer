"""Everything the healer does to the cluster: evict, force-delete, cordon/uncordon, record events, notify."""
from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.rest import ApiException

log = logging.getLogger("healer.actions")


class Actions:
    def __init__(self, core: client.CoreV1Api, dry_run: bool, webhook_url: str | None, component: str = "k8s-auto-healer"):
        self.core = core
        self.dry_run = dry_run
        self.webhook_url = webhook_url
        self.component = component

    def evict(self, pod) -> str:
        """Evict through the Eviction API, so PodDisruptionBudgets are honoured.

        Returns "evicted", "dry_run" or "blocked_by_pdb". A plain delete would ignore the
        workload's PDB; eviction is refused (HTTP 429) when it would break the budget.
        """
        name, ns = pod.metadata.name, pod.metadata.namespace
        if self.dry_run:
            return "dry_run"
        body = client.V1Eviction(metadata=client.V1ObjectMeta(name=name, namespace=ns))
        try:
            self.core.create_namespaced_pod_eviction(name=name, namespace=ns, body=body)
            return "evicted"
        except ApiException as e:
            if e.status == 429:
                return "blocked_by_pdb"
            if e.status == 404:
                return "already_gone"
            raise

    def force_delete(self, pod) -> str:
        """Delete with grace period 0: for pods stuck Terminating on an unreachable node."""
        if self.dry_run:
            return "dry_run"
        try:
            self.core.delete_namespaced_pod(pod.metadata.name, pod.metadata.namespace,
                                            body=client.V1DeleteOptions(grace_period_seconds=0))
            return "force_deleted"
        except ApiException as e:
            if e.status == 404:
                return "already_gone"
            raise

    def cordon(self, node, identity: str, reason: str) -> str:
        """Mark unschedulable and record who did it and why (only our cordons are ever undone)."""
        if self.dry_run:
            return "dry_run"
        self.core.patch_node(node.metadata.name, {
            "metadata": {"annotations": {"autohealer.io/cordoned-by": identity, "autohealer.io/cordon-reason": reason}},
            "spec": {"unschedulable": True}})
        return "cordoned"

    def uncordon(self, node) -> str:
        if self.dry_run:
            return "dry_run"
        self.core.patch_node(node.metadata.name, {
            "metadata": {"annotations": {"autohealer.io/cordoned-by": None, "autohealer.io/cordon-reason": None}},
            "spec": {"unschedulable": False}})
        return "uncordoned"

    def node_event(self, node, reason: str, message: str, type_: str = "Warning") -> None:
        """Events about cluster-scoped objects live in the default namespace."""
        ref = client.V1ObjectReference(kind="Node", name=node.metadata.name, uid=node.metadata.uid, api_version="v1")
        self._event("default", ref, node.metadata.name, reason, message, type_)

    def event(self, pod, reason: str, message: str, type_: str = "Warning") -> None:
        """A Kubernetes Event on the pod: shows up in `kubectl describe pod` and event exporters."""
        ref = client.V1ObjectReference(kind="Pod", name=pod.metadata.name, namespace=pod.metadata.namespace,
                                       uid=pod.metadata.uid, api_version="v1")
        self._event(pod.metadata.namespace, ref, pod.metadata.name, reason, message, type_)

    def _event(self, namespace: str, ref, name: str, reason: str, message: str, type_: str) -> None:
        now = datetime.now(timezone.utc)
        ev = client.CoreV1Event(
            metadata=client.V1ObjectMeta(generate_name=f"{name}.", namespace=namespace), involved_object=ref,
            reason=reason, message=message, type=type_, first_timestamp=now, last_timestamp=now, count=1,
            source=client.V1EventSource(component=self.component), reporting_component=self.component,
        )
        try:
            self.core.create_namespaced_event(namespace, ev)
        except ApiException as e:
            log.warning("could not record event on %s: %s", name, e.reason)

    def notify(self, text: str) -> None:
        """Optional Slack-compatible webhook ({"text": ...}); failures never stop the healer."""
        if not self.webhook_url:
            return
        req = urllib.request.Request(self.webhook_url, data=json.dumps({"text": text}).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5).close()
        except Exception as e:  # noqa: BLE001 - notification is best effort
            log.warning("webhook notification failed: %s", e)
