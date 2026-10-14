#!/usr/bin/env bash
# Node failure experiment, in a throwaway kind cluster (1 control plane + 3 workers):
# freeze a worker that runs replicas of the demo service (`docker pause`: kubelet, pods and
# network all stop, like a hung or partitioned machine) and measure how long until all 3
# replicas are Ready again on healthy nodes.
#
#   A) Kubernetes alone: the node goes NotReady, then the node controller evicts its pods only
#      after their not-ready toleration (300 s by default), and replacements start after that.
#   B) With the healer's node remediation (NotReady for 20 s -> cordon + drain).
# Then: the node comes back -> the healer uncordons it (it cordoned it) after 30 s healthy.
# Then: 2 of 3 workers freeze at once -> the circuit breaker must block all node actions.
#
# The demo pods run WITHOUT the functional-check annotation here, so only node remediation can
# act (with it, the pod loop evicts the frozen pod after 3 failed checks, which also recovers the
# service but isn't what this experiment measures).
#
# Uses its own cluster and kubeconfig (never the current context); deleted at the end (KEEP=1 keeps it).
#   bash scripts/node-failure-experiment.sh
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-healer-nodes}
NS=healer-demo
WINDOW_A=${WINDOW_A:-420}
WINDOW_B=${WINDOW_B:-240}
export KUBECONFIG="${TMPDIR:-/tmp}/kubeconfig-$CLUSTER"
k() { kubectl --context "kind-$CLUSTER" "$@"; }
PAUSED=()

cleanup() {
  for n in "${PAUSED[@]}"; do docker unpause "$n" >/dev/null 2>&1 || true; done
  [ -n "${KEEP:-}" ] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log() { printf '\n== %s\n' "$*"; }

log "cluster"
if ! kind get clusters | grep -qx "$CLUSTER"; then
  cat <<EOF | kind create cluster --name "$CLUSTER" --image "${KIND_NODE:-kindest/node:v1.34.0}" --wait 180s --config - >/dev/null
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes: [{role: control-plane}, {role: worker}, {role: worker}, {role: worker}]
EOF
fi
kind load docker-image healer:local healer-demo-app:local --name "$CLUSTER" >/dev/null
k create namespace "$NS" --dry-run=client -o yaml | k apply -f - >/dev/null
k create namespace healer-system --dry-run=client -o yaml | k apply -f - >/dev/null

deploy_demo() {
  k -n "$NS" delete deploy shop --ignore-not-found --wait >/dev/null
  grep -v 'autohealer.io/' demo/manifests/demo.yaml | k -n "$NS" apply -f - >/dev/null
  k -n "$NS" rollout status deploy/shop --timeout=180s >/dev/null
}

victim() {  # the worker running the most shop replicas
  k -n "$NS" get pods -l app=shop -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort | uniq -c | sort -rn | awk 'NR==1{print $2}'
}

ready_elsewhere() {  # Ready, not terminating, shop replicas on nodes other than $1
  k -n "$NS" get pods -l app=shop -o json | python3 -c '
import json, sys
bad = sys.argv[1]
n = 0
for p in json.load(sys.stdin)["items"]:
    if p["metadata"].get("deletionTimestamp") or p["spec"].get("nodeName") == bad:
        continue
    if any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", [])):
        n += 1
print(n)' "$1"
}

measure() {  # measure <label> <window>: freeze the victim node, time until 3 replicas are Ready elsewhere
  local label=$1 window=$2 node t0 now notready=""; cordoned_at=""
  node=$(victim)
  echo "   freezing $node (runs $(k -n "$NS" get pods -l app=shop --field-selector spec.nodeName="$node" --no-headers | wc -l) of 3 replicas)"
  docker pause "$node" >/dev/null; PAUSED+=("$node")
  t0=$(date +%s)
  while :; do
    now=$(( $(date +%s) - t0 ))
    if [ -z "$notready" ] && [ "$(k get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" != True ]; then
      notready=$now; echo "   ${now}s node NotReady"
    fi
    if [ -z "${cordoned_at:-}" ] && [ "$(k get node "$node" -o jsonpath='{.spec.unschedulable}')" = true ]; then
      cordoned_at=$now; echo "   ${now}s node cordoned ($(k get node "$node" -o jsonpath='{.metadata.annotations.autohealer\.io/cordon-reason}'))"
    fi
    if [ "$(ready_elsewhere "$node")" -ge 3 ]; then
      echo "   ${now}s all 3 replicas Ready on healthy nodes  (RECOVERED)"; echo "$now" > "/tmp/node-result-$label"; break
    fi
    if [ "$now" -ge "$window" ]; then
      echo "   ${window}s: only $(ready_elsewhere "$node") of 3 replicas Ready elsewhere  (NOT RECOVERED)"; echo "-1" > "/tmp/node-result-$label"; break
    fi
    sleep 3
  done
  echo "$node" > /tmp/node-victim
}

unfreeze() {
  docker unpause "$1" >/dev/null; PAUSED=("${PAUSED[@]/$1}")
  k wait --for=condition=Ready "node/$1" --timeout=180s >/dev/null
}

if [ -z "${SKIP_BASELINE:-}" ]; then   # SKIP_BASELINE=1: reuse the last Kubernetes-alone result
  log "A) Kubernetes alone"
  deploy_demo
  measure without-healer "$WINDOW_A"
  unfreeze "$(cat /tmp/node-victim)"
fi

log "B) With the healer's node remediation"
helm upgrade --install healer chart/k8s-auto-healer --kube-context "kind-$CLUSTER" -n healer-system \
  --set image.repository=healer --set image.tag=local --set image.pullPolicy=IfNotPresent \
  --set "watchNamespaces={$NS}" --set intervalSeconds=5 --set stuckTerminatingSeconds=20 \
  --set nodeRemediation.enabled=true --set nodeRemediation.notReadySeconds=20 \
  --set nodeRemediation.recoverySeconds=30 \
  --set 'nodeSelector.node-role\.kubernetes\.io/control-plane=' \
  --set 'tolerations[0].key=node-role.kubernetes.io/control-plane' --set 'tolerations[0].operator=Exists' \
  --set 'tolerations[0].effect=NoSchedule' --wait --timeout 180s >/dev/null
deploy_demo
sleep 10
measure with-healer "$WINDOW_B"
node=$(cat /tmp/node-victim)
for _ in $(seq 20); do [ -n "$(k get node "$node" -o jsonpath='{.metadata.annotations.autohealer\.io/cordoned-by}')" ] && break; sleep 3; done
by=$(k get node "$node" -o jsonpath='{.metadata.annotations.autohealer\.io/cordoned-by}')
echo "   cordoned by: ${by:-<not cordoned>}; event: $(k get events -n default --field-selector involvedObject.name="$node",reason=NodeCordoned -o jsonpath='{.items[0].message}')"

log "Recovery: the node comes back"
unfreeze "$node"
uncordoned=""
[ -n "$by" ] && for i in $(seq 40); do
  if [ "$(k get node "$node" -o jsonpath='{.spec.unschedulable}')" != true ]; then uncordoned=$((i * 3)); break; fi
  sleep 3
done
[ -n "$uncordoned" ] && echo "   uncordoned by the healer ~${uncordoned}s after the node was Ready again" || echo "   NOT uncordoned"

log "Circuit breaker: 2 of 3 workers freeze at once"
workers=($(k get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}'))
docker pause "${workers[0]}" "${workers[1]}" >/dev/null; PAUSED+=("${workers[0]}" "${workers[1]}")
sleep 90
cordoned=$(k get nodes -o jsonpath='{range .items[*]}{.metadata.name}={.spec.unschedulable}{"\n"}{end}' | grep -c '=true' || true)
# Read every healer replica: the leader may be the one on a frozen node (its logs can't be
# fetched while the kubelet is down); the standby then takes the Lease and acts.
paused_log=$(for p in $(k -n healer-system get pods -o name); do k -n healer-system logs "$p" --request-timeout=10s 2>/dev/null; done | grep -m1 "breaker_open" || true)
echo "   nodes cordoned: $cordoned; healer log: ${paused_log:-<none>}"
breaker_ok=$([ "$cordoned" = 0 ] && [ -n "$paused_log" ] && echo yes || echo no)
docker unpause "${workers[0]}" "${workers[1]}" >/dev/null; PAUSED=()

A=$(cat /tmp/node-result-without-healer); B=$(cat /tmp/node-result-with-healer)
fmt() { [ "$1" = -1 ] && echo "not within $2 s" || echo "$1 s"; }
echo
echo "| Same node failure | All 3 replicas Ready on healthy nodes after |"
echo "|---|---|"
echo "| Kubernetes alone | $(fmt "$A" "$WINDOW_A") |"
echo "| With node remediation | $(fmt "$B" "$WINDOW_B") |"
echo "| Uncordon after recovery | $([ -n "$uncordoned" ] && echo "~${uncordoned} s after Ready (recoverySeconds=30)" || echo no) |"
echo "| Breaker with 2 of 3 workers down | $([ "$breaker_ok" = yes ] && echo 'open, nothing cordoned' || echo 'FAILED') |"
[ "$B" != -1 ] && [ -n "$by" ] && [ -n "$uncordoned" ] && [ "$breaker_ok" = yes ]
