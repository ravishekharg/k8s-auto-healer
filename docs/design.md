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

Every action and every suppressed action is visible: Prometheus counters
(`healer_remediations_total`, `healer_suppressed_total{reason}`), a Kubernetes Event on the pod,
and an optional webhook message.

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
Watch API (informers) instead of polling for very large clusters; per-namespace budgets;
a CRD (`HealingPolicy`) instead of annotations; an exemplar trace link in events; integration
with PagerDuty when the global budget trips.
