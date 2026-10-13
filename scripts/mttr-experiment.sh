#!/usr/bin/env bash
# MTTR experiment: the same failure, without and with the healer.
#
# A demo service runs 3 replicas behind a Service, with traffic from inside the cluster every
# 0.2 s. One pod is made to hang (its worker is stuck; /health and /ready keep passing). We then
# measure how many requests fail and how long until failures stop:
#   A) Kubernetes alone: liveness and readiness never fail, so the pod is never replaced.
#   B) With the healer (functional check on /work every 5 s, threshold 3).
#
# Needs: kubectl, helm, and the images healer:local and healer-demo-app:local available to the
# cluster (kind: `kind load docker-image ...`). Uses its own namespaces and removes them after.
#   KUBE_CONTEXT=kind-healer scripts/mttr-experiment.sh
#   WINDOW=180 scripts/mttr-experiment.sh
set -euo pipefail
cd "$(dirname "$0")/.."
CONTEXT=${KUBE_CONTEXT:-$(kubectl config current-context)}
DEMO_NS=${DEMO_NS:-healer-demo}
HEALER_NS=${HEALER_NS:-healer-system}
WINDOW=${WINDOW:-120}          # seconds observed after the hang
k() { command kubectl --context "$CONTEXT" "$@"; }
h() { command helm --kube-context "$CONTEXT" "$@"; }

cleanup() {
  h uninstall healer -n "$HEALER_NS" >/dev/null 2>&1 || true
  k delete namespace "$DEMO_NS" "$HEALER_NS" --wait=false >/dev/null 2>&1 || true
}
[ "${KEEP:-}" = "1" ] || trap cleanup EXIT

echo "== Deploying the demo service ($DEMO_NS) on context $CONTEXT"
k create namespace "$DEMO_NS" >/dev/null 2>&1 || true
k create namespace "$HEALER_NS" >/dev/null 2>&1 || true
k -n "$DEMO_NS" apply -f demo/manifests/demo.yaml >/dev/null
k -n "$DEMO_NS" rollout status deploy/shop --timeout=180s >/dev/null

start_traffic() {
  k -n "$DEMO_NS" delete pod traffic --ignore-not-found --wait=true >/dev/null
  k -n "$DEMO_NS" run traffic --image=curlimages/curl:8.11.1 --restart=Never --command -- sh -c \
    'while true; do printf "%s %s\n" "$(date +%s)" "$(curl -s -o /dev/null -m 2 -w "%{http_code}" http://shop/work)"; sleep 0.2; done' >/dev/null
  k -n "$DEMO_NS" wait --for=condition=Ready pod/traffic --timeout=90s >/dev/null
}

hang_one_pod() {
  local pod ip
  pod=$(k -n "$DEMO_NS" get pods -l app=shop -o jsonpath='{.items[0].metadata.name}')
  ip=$(k -n "$DEMO_NS" get pod "$pod" -o jsonpath='{.status.podIP}')
  k -n "$DEMO_NS" exec traffic -- curl -s -X POST "http://$ip:8080/hang" >/dev/null
  echo "$pod"
}

measure() {   # measure <label>: hang a pod, watch for WINDOW seconds, summarise
  local label=$1 pod t0
  start_traffic
  sleep 10
  t0=$(date +%s)
  pod=$(hang_one_pod)
  echo "   hung $pod at $(date -u +%T); observing ${WINDOW}s"
  sleep "$WINDOW"
  k -n "$DEMO_NS" logs traffic > /tmp/traffic-$label.log
  python3 - "$t0" "$WINDOW" "$label" /tmp/traffic-$label.log <<'EOF'
import sys
t0, window, label, path = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4]
rows = [l.split() for l in open(path) if len(l.split()) == 2]
after = [(int(t), c) for t, c in rows if int(t) >= t0]
failed = [t for t, c in after if c != "200"]
last = max(failed) if failed else None
recovered = last is not None and last < t0 + window - 15
print(f"   requests after the hang: {len(after)}, failed: {len(failed)}")
if not failed:
    print("   no failures observed")
elif recovered:
    print(f"   failures stopped {last - t0} s after the hang  (RECOVERED)")
else:
    print(f"   still failing at the end of the {window} s window  (NOT RECOVERED)")
open(f"/tmp/result-{label}", "w").write(f"{len(after)} {len(failed)} {(last - t0) if recovered else -1}\n")
EOF
}

echo "== A) Kubernetes alone"
measure without-healer
k -n "$DEMO_NS" get pods -l app=shop --no-headers | awk '{print "   " $1, $2, $3, "restarts=" $4}'
k -n "$DEMO_NS" rollout restart deploy/shop >/dev/null && k -n "$DEMO_NS" rollout status deploy/shop --timeout=180s >/dev/null

echo "== B) With the healer"
h upgrade --install healer chart/k8s-auto-healer -n "$HEALER_NS" \
  --set image.repository=healer --set image.tag=local --set image.pullPolicy=IfNotPresent \
  --set "watchNamespaces={$DEMO_NS}" --set intervalSeconds=5 --set failureThreshold=3 \
  --set startupGraceSeconds=10 --set cooldownSeconds=30 --wait --timeout 180s >/dev/null
sleep 15
measure with-healer
echo "   healer events:"
k -n "$DEMO_NS" get events --field-selector reason=FunctionalCheckFailed -o custom-columns=MSG:.message --no-headers | sed 's/^/     /'

read -r A_REQ A_FAIL A_REC < /tmp/result-without-healer
read -r B_REQ B_FAIL B_REC < /tmp/result-with-healer
echo
echo "| Run | Requests | Failed | Failures stopped after |"
echo "|---|---|---|---|"
echo "| Kubernetes alone | $A_REQ | $A_FAIL | $([ "$A_REC" = -1 ] && echo "not within ${WINDOW} s" || echo "${A_REC} s") |"
echo "| With the healer | $B_REQ | $B_FAIL | $([ "$B_REC" = -1 ] && echo "not within ${WINDOW} s" || echo "${B_REC} s") |"
[ "$B_REC" != -1 ]
