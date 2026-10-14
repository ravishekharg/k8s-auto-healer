# Design notes

## Why not just a liveness probe?
A liveness probe answers "is the process alive?" and restarts the **container** when it isn't.
It's the wrong tool for "the pod is alive but its real work is failing" because:
- Pointing liveness at a deep check (database, downstream, worker pool) is dangerous: when the
  shared dependency blips, **every replica fails liveness at once and Kubernetes restarts all of
  them together**, turning a dependency hiccup into a full outage.
- Probes run per container with no cluster-wide view: no "one at a time", no budget, no PDB.
- Many real hangs leave the probe endpoint healthy (it runs on a different thread or event loop
  than the stuck work): the demo app reproduces exactly that.

The healer keeps liveness shallow and adds a separate, deliberate remediation path with the
safety rules probes lack.

## Safety model
| Rule | Prevents |
|---|---|
| Opt-in per pod template (`autohealer.io/check-path`) | Touching workloads whose owners didn't ask for it |
| N consecutive failures (default 3) | Acting on one slow request |
| Startup grace (default 60 s) | Judging pods still warming up |
| One remediation per workload at a time + cooldown | Taking several replicas of a service down together; acting again before the replacement is up |
| Eviction API, not delete | Breaking the workload's PodDisruptionBudget (eviction returns 429 instead) |
| Global hourly budget (default 10) | Churning the whole cluster during a systemic outage (e.g. the database is down and every pod fails): that needs a human, so further actions are held back and counted |
| Dry-run mode | Surprises when introducing it: log and record events first |
| Bare pods and StatefulSet force-deletes excluded | Losing pods nothing would recreate; two pods with one StatefulSet identity |
| Leader election (Lease) | Two replicas acting on the same pod |
| Circuit breaker "pods": open when over half the checked pods fail in one loop | Evicting pods that are all blocked on one dead dependency (they'd fail again on restart, and the churn hides the real cause) |
| Circuit breaker "nodes": open when over a third of worker nodes are unhealthy | Draining a cluster during a network partition or control-plane problem, when the nodes are fine and the view of them is wrong |

Every action and every suppressed action is visible: Prometheus counters
(`healer_remediations_total`, `healer_suppressed_total{reason}`), a Kubernetes Event on the pod,
and an optional webhook message.

## Node remediation (opt-in)
`nodeRemediation.enabled=true` adds a node loop ([`healer/nodes.py`](../healer/nodes.py)):

| Node state, past its threshold | Action |
|---|---|
| NotReady (Ready False/Unknown) | Cordon, drain (Eviction API, PDB-aware); the pod loop's stuck-Terminating handling frees ReplicaSet pods left on the dead kubelet |
| Memory/Disk/PID pressure | Cordon and drain *before* the kubelet's own hard evictions, which ignore PDBs |
| Healthy again for `recoverySeconds` | Uncordon, **only** if this healer cordoned it (annotation `autohealer.io/cordoned-by`) |

Why bother when Kubernetes evicts pods from NotReady nodes itself: it waits for the pods'
`not-ready` toleration (300 s by default) and only then do replacements start; the experiment
measures the difference. Lowering the toleration cluster-wide is the other option, but it also
evicts on every short kubelet blip, with no one-node-at-a-time limit, budget or breaker.

Safety: one node at a time (finish draining before the next), a separate hourly budget (default
2), control-plane nodes and `autohealer.io/ignore=true` nodes never touched, DaemonSet and
static pods never evicted, and the nodes circuit breaker above.

### The circuit breaker
[`healer/breaker.py`](../healer/breaker.py): CLOSED -> OPEN on a systemic signal (too many
unhealthy nodes / failing pods) or 3 consecutive failed actions (API errors); OPEN -> HALF_OPEN
after a reset timeout, which allows exactly one trial action; its success closes the breaker, its
failure re-opens it. A breaker opened by the systemic signal closes as soon as the signal clears.
Every opening sends one notification ("PAUSED ... needs a human") and sets
`healer_circuit_open{scope}`; repeated trips while open don't re-alert.

## What it deliberately doesn't do
- **Restart crash-looping pods**: kubelet already restarts them; the problem is the code, config,
  image or memory limit. The healer reports it once an hour with the last exit reason (e.g.
  `OOMKilled (exit 137)`).
- **Scale, roll back, or change resources**: those are decisions for HPA/VPA, Argo Rollouts or a
  person.

## Alternatives and when they fit
| Tool | Fits when |
|---|---|
| Well-designed probes (startup + shallow liveness + readiness) | Always: the first line of defence |
| Alertmanager → webhook → automation (e.g. Robusta, StackStorm) | You want remediation driven by existing alerts and runbooks |
| Kubernetes descheduler | Rebalancing pods across nodes, evicting pods violating policies |
| Operator for a specific system (CloudNativePG, Strimzi) | Remediation needs domain knowledge (failover, rebalancing) |
| Service mesh outlier detection (Envoy) | Stop *sending traffic* to a bad pod without replacing it (complements this) |

## Production next steps
Node replacement rather than just drain (delete the Node so Karpenter / the node group replaces
the instance, or hand over to Cluster API's MachineHealthCheck);
Watch API (informers) instead of polling for very large clusters; per-namespace budgets;
a CRD (`HealingPolicy`) instead of annotations; an exemplar trace link in events; integration
with PagerDuty when the global budget trips.
