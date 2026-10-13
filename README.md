# k8s-auto-healer

A small Kubernetes controller that finds pods which are **Running and Ready but no longer doing
their job**, and replaces them safely. Kubernetes restarts containers whose liveness probe fails,
but a pod whose worker is deadlocked, whose connection pool has leaked, or whose event loop is
stuck behind a passing `/health` endpoint keeps receiving traffic and failing requests
indefinitely. This controller checks the path that does real work, and when a pod fails it
repeatedly, evicts it so its Deployment starts a fresh one, one replica at a time, within the
workload's PodDisruptionBudget, under a global action budget.

It's a personal project rebuilding, in the open, the kind of Python tooling I wrote for
production Kubernetes platforms (proactive health checks and automated pod recovery to cut MTTR).
It isn't code from a former employer.

## Result

`scripts/mttr-experiment.sh`: 3 replicas behind a Service, traffic every 0.2 s from inside the
cluster, one pod made to hang (its worker is stuck; `/health` and `/ready` keep passing).
Measured 2026-10-13 on a kind cluster (Kubernetes 1.34); CI runs the same experiment on every push.

| Same failure | Requests (120 s) | Failed | Failures stopped after |
|---|---|---|---|
| Kubernetes alone | 260 | 65 (25%) | **never**: still failing at 120 s, pod `1/1 Running`, 0 restarts |
| With the healer (check every 5 s, threshold 3) | 512 | **10** | **16 s** |

The event the healer left on the pod: `failed functional check /work on port 8080 3 times in a
row; evicted so its controller replaces it`.

## How it works

```
every interval (leader only, via a coordination.k8s.io Lease):
  list pods in the watched namespaces
  for each opted-in pod that is Running, past its startup grace and owned by a controller:
      GET http://<pod IP>:<port><check-path>   (in parallel, with a timeout)
      Policy: N consecutive failures?
              another replica of this workload being replaced, or in cooldown?  → hold back
              global hourly budget spent?                                        → hold back
              otherwise → Eviction API (PDB-aware) + Event + webhook
  ReplicaSet pods stuck Terminating long past their grace period → force delete
  pods in CrashLoopBackOff → report (Event + webhook, with the last exit reason), never restart
```

Opt-in, per pod template:
```yaml
metadata:
  annotations:
    autohealer.io/check-path: /work        # a path that exercises the real work
    autohealer.io/check-port: "8080"       # default: first container port
    autohealer.io/failure-threshold: "3"   # default: chart value
    autohealer.io/timeout-seconds: "2"
```

| File | What's in it |
|---|---|
| [`healer/policy.py`](healer/policy.py) | Pure decision logic: consecutive-failure tracking, one remediation per workload + cooldown, sliding-window global budget |
| [`healer/pods.py`](healer/pods.py) | Reading opt-in annotations, the owning workload (ReplicaSet → Deployment), timings, crash loops and last exit reasons |
| [`healer/actions.py`](healer/actions.py) | Eviction (429 = blocked by the PDB, respected), force delete, Kubernetes Events, webhook |
| [`healer/leader.py`](healer/leader.py) | Lease-based leader election with optimistic concurrency (409 = someone else won) |
| [`healer/main.py`](healer/main.py) | The reconcile loop and Prometheus metrics |
| [`chart/`](chart/k8s-auto-healer) | Helm chart: 2 replicas, least-privilege RBAC (ClusterRole for all namespaces, or Roles only in `watchNamespaces`), non-root, read-only filesystem |
| [`demo/`](demo) | The hangable demo service and its manifests (Deployment, Service, PDB) |
| [`docs/design.md`](docs/design.md) | Why not a liveness probe, the safety model, what it deliberately doesn't do, alternatives |

Metrics: `healer_checks_total{result}`, `healer_remediations_total{reason,result}`,
`healer_suppressed_total{reason}`, `healer_crashloops_detected_total`, `healer_is_leader`,
`healer_loop_seconds`.

## Run it

```bash
python -m pytest -q tests                      # 19 unit tests: policy, pod parsing, leader election
kind create cluster --name healer
docker build -t healer:local . && docker build -t healer-demo-app:local demo/app
kind load docker-image healer:local healer-demo-app:local --name healer
KUBE_CONTEXT=kind-healer scripts/mttr-experiment.sh

# Install in a cluster (start in dry-run: it logs and records events without acting)
helm install healer chart/k8s-auto-healer -n healer-system --create-namespace --set dryRun=true
```

## Status
| Part | Status |
|---|---|
| Unit tests | 19 passing |
| Functional-check remediation | Verified by the experiment above (local kind and CI) |
| Leader election | Unit-tested against a fake Lease API with resourceVersion conflicts |
| Stuck-Terminating and CrashLoop reporting | Implemented and unit-tested at the parsing level; not exercised in the experiment |
| Scale | Polls the API every interval; for very large clusters use watches/informers (see design notes) |
