#!/usr/bin/env bash
# Kind end-to-end test for the control plane (ADR 0010).
#
# Creates a 4-node Kind cluster, installs the chart with values-kind.yaml, and
# verifies through the public interfaces (gpuctl, the status page, kubectl):
#
#   1. install and a real container job end to end (busybox in gpub-jobs)
#   2. rolling upgrade with no failed API requests, then helm rollback
#   3. API, agent and postgres-free pod restarts mid-job
#   4. node drain: the attempt is LOST and retried on the other GPU node
#   5. scheduler leader replacement after a forced pod deletion
#   6. the public status page refuses writes
#
# Results are written as JSON to $OUT (default /tmp/gpub-kind-e2e.json).
# Requires docker, kind, kubectl, helm, go. Set KEEP=1 to keep the cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
CP="$ROOT/controlplane"
CHART="$ROOT/infra/helm/gpu-broker"
CLUSTER=gpub
NS=gpub
OUT="${OUT:-${TMPDIR:-/tmp}/gpub-kind-e2e.json}"
BIN="$(mktemp -d)"
# Avoid other local brokers, including IPv6 listeners on common dev ports.
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
API_PORT=$(free_port)
STATUS_PORT=$(free_port)
while [[ "$STATUS_PORT" == "$API_PORT" ]]; do STATUS_PORT=$(free_port); done
RESULTS=()
mkdir -p "$(dirname "$OUT")"

log() { printf '\n== %s\n' "$*" >&2; }
record() { RESULTS+=("$1"); }
fail() { echo "FAIL: $*" >&2; dump; exit 1; }
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

dump() {
  echo "--- diagnostics" >&2
  kubectl -n "$NS" get pods -o wide >&2 || true
  kubectl -n gpub-jobs get jobs,pods -o wide >&2 || true
  kubectl -n "$NS" logs deploy/gb-scheduler --tail=30 >&2 || true
  kubectl -n "$NS" logs ds/gb-agent --tail=30 >&2 || true
}

cleanup() {
  [[ -n "${PF_API:-}" ]] && kill "$PF_API" 2>/dev/null || true
  [[ -n "${PF_STATUS:-}" ]] && kill "$PF_STATUS" 2>/dev/null || true
  kubectl -n "$NS" delete pod gb-rollout-probe --ignore-not-found >/dev/null 2>&1 || true
  if [[ "${KEEP:-0}" != 1 ]]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

admin() { kubectl -n "$NS" exec deploy/gb-api -c api -- gpubroker admin "$@" --metrics-addr=; }

port_forward() {
  [[ -n "${PF_API:-}" ]] && kill "$PF_API" 2>/dev/null || true
  [[ -n "${PF_STATUS:-}" ]] && kill "$PF_STATUS" 2>/dev/null || true
  kubectl -n "$NS" port-forward svc/gb-api "$API_PORT:8080" --address=127.0.0.1 >"$BIN/api-forward.log" 2>&1 & PF_API=$!
  kubectl -n "$NS" port-forward svc/gb-status "$STATUS_PORT:8081" --address=127.0.0.1 >"$BIN/status-forward.log" 2>&1 & PF_STATUS=$!
  for _ in $(seq 1 60); do
    kill -0 "$PF_API" 2>/dev/null && kill -0 "$PF_STATUS" 2>/dev/null || {
      cat "$BIN/api-forward.log" "$BIN/status-forward.log" >&2
      fail "port-forward exited before readiness"
    }
    curl -sf "127.0.0.1:$API_PORT/readyz" >/dev/null && curl -sf "127.0.0.1:$STATUS_PORT/healthz" >/dev/null && return
    sleep 1
  done
  fail "port-forward never became ready"
}

# Consume the entire response so pipefail does not turn a successful CLI call
# into SIGPIPE when status includes attempt details after its first line.
job_state() { "$BIN/gpuctl" status "$1" | sed -n '1p' | awk '{print $2}'; }

wait_state() { # job, wanted state, timeout seconds
  for _ in $(seq 1 "$3"); do
    s=$(job_state "$1" 2>/dev/null || echo "?")
    [[ "$s" == "$2" ]] && return 0
    case "$s" in SUCCEEDED|FAILED|CANCELLED|EXPIRED) [[ "$s" != "$2" ]] && fail "$1 ended $s, wanted $2" ;; esac
    sleep 1
  done
  fail "$1 did not reach $2 (last $s)"
}

submit() { "$BIN/gpuctl" submit --image gpub-e2e-busybox:1 --gpus "$1" --max-runtime 10m --runtime kubernetes -- "${@:2}" | awk '{print $1}'; }

log "build"
(cd "$CP" && go build -o "$BIN/" ./cmd/gpuctl)
docker build -q -t gpubroker:dev "$CP" >/dev/null
# A single-platform re-tag: kind cannot import busybox's multi-platform index.
echo 'FROM busybox:1.36' | docker build -q -t gpub-e2e-busybox:1 - >/dev/null

log "cluster"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --config "$CP/tests/e2e/kind-config.yaml" --wait 120s
kind load docker-image gpubroker:dev gpub-e2e-busybox:1 --name "$CLUSTER" >/dev/null
GPU_NODES=($(kubectl get nodes -l gpub.dev/pool=kind-london -o jsonpath='{.items[*].metadata.name}'))

log "1. install"
t0=$(now_ms)
helm upgrade --install gb "$CHART" -n "$NS" --create-namespace -f "$CHART/values-kind.yaml" --wait --timeout 6m
kubectl -n "$NS" wait --for=condition=complete job/gb-migrate-1 --timeout=120s
kubectl -n "$NS" rollout status ds/gb-agent --timeout=120s
record "{\"step\":\"install\",\"ms\":$(( $(now_ms) - t0 ))}"
port_forward
TOKEN=$(admin create-tenant --name e2e --handle e2e --budget-usd 100 | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
export GPUB_API_URL=http://127.0.0.1:$API_PORT GPUB_TOKEN="$TOKEN"
for _ in $(seq 1 60); do "$BIN/gpuctl" doctor >/dev/null 2>&1 && break; sleep 1; done
"$BIN/gpuctl" doctor
"$BIN/gpuctl" pools

J1=$(submit 1 sh -c 'echo hello-from-attempt; sleep 3; echo done')
t0=$(now_ms)
wait_state "$J1" SUCCEEDED 120
"$BIN/gpuctl" logs "$J1" | grep hello-from-attempt >/dev/null || fail "attempt log line missing"
"$BIN/gpuctl" policy explain "$J1" | sed -n '1,8p'
record "{\"step\":\"real_container_job\",\"job_ms\":$(( $(now_ms) - t0 ))}"
kubectl -n gpub-jobs get pods -o jsonpath='{.items[0].spec.securityContext.runAsNonRoot}{" "}{.items[0].spec.automountServiceAccountToken}' | grep -q 'true false' \
  || fail "attempt pod is not running with the isolation boundary"

log "2. rolling upgrade under traffic, then rollback"
# A port-forward pins one pod, so it cannot test Service availability.
# This client stays inside the cluster and reaches the Service through kube-proxy.
kubectl -n "$NS" run gb-rollout-probe --image=gpub-e2e-busybox:1 \
  --image-pull-policy=Never --restart=Never --env="PROBE_TOKEN=$TOKEN" \
  --command -- sh -c '
    while [ ! -f /tmp/stop ]; do
      response=$(wget -T 3 -S -O /dev/null --header="Authorization: Bearer $PROBE_TOKEN" http://gb-api:8080/v1/pools 2>&1)
      code=$(printf "%s\n" "$response" | awk "/HTTP\// {print \$2; exit}")
      echo "${code:-000}"
      case "$code" in 200) ;; *) printf "%s\n" "$response" >&2 ;; esac
      sleep 0.2
    done'
kubectl -n "$NS" wait --for=condition=Ready pod/gb-rollout-probe --timeout=60s
t0=$(now_ms)
helm upgrade gb "$CHART" -n "$NS" -f "$CHART/values-kind.yaml" --set-string podAnnotations.rollout=rev2 --set scheduler.interval=2s --wait --timeout 5m
kubectl -n "$NS" rollout status deploy/gb-api --timeout=120s
kubectl -n "$NS" rollout status deploy/gb-scheduler --timeout=120s
upgrade_ms=$(( $(now_ms) - t0 ))
helm rollback gb 1 -n "$NS" --wait --timeout 5m
kubectl -n "$NS" rollout status deploy/gb-scheduler --timeout=120s
kubectl -n "$NS" exec gb-rollout-probe -- touch /tmp/stop
kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/gb-rollout-probe --timeout=30s
kubectl -n "$NS" logs gb-rollout-probe > "$BIN/probe.log"
probes=$(wc -l < "$BIN/probe.log" | tr -d ' ')
probe_errors=$(grep -c -v '^200$' "$BIN/probe.log" || true)
port_forward
[[ "$probes" -gt 0 ]] || fail "rollout probe recorded no requests"
[[ "$probe_errors" == 0 ]] || fail "$probe_errors failed Service requests during rollout"
kubectl -n "$NS" delete pod gb-rollout-probe >/dev/null
J2=$(submit 1 sh -c 'echo after-rollback')
wait_state "$J2" SUCCEEDED 120
record "{\"step\":\"upgrade_rollback\",\"upgrade_ms\":$upgrade_ms,\"probes\":$probes,\"service_errors\":$probe_errors,\"revision_after\":$(helm history gb -n $NS -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)[-1]["revision"])')}"

log "3. pod restarts mid-job (API and the job's agent)"
J3=$(submit 1 sh -c 'sleep 25; echo survived')
wait_state "$J3" RUNNING 60
node=$(kubectl -n gpub-jobs get pods -l "gpub.dev/job=$(echo "$J3" | tr '_' '-')" -o jsonpath='{.items[0].spec.nodeName}')
agent=$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=agent --field-selector "spec.nodeName=$node" -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" delete pod "$agent" --wait=false
kubectl -n "$NS" delete pod -l app.kubernetes.io/component=api --wait=false
sleep 5; port_forward
wait_state "$J3" SUCCEEDED 120
attempts=$("$BIN/gpuctl" status "$J3" | sed -n '1p' | grep -o 'attempt=[0-9]*/' | tr -dc 0-9)
[[ "$attempts" == 1 ]] || fail "agent restart caused a retry (attempt $attempts); it should re-adopt the running Job"
record "{\"step\":\"pod_restart\",\"node\":\"$node\",\"attempts\":$attempts}"

log "4. node drain moves the work"
J4=$(submit 1 sh -c 'sleep 40; echo finished')
wait_state "$J4" RUNNING 60
node=$(kubectl -n gpub-jobs get pods -l "gpub.dev/job=$(echo "$J4" | tr '_' '-')" -o jsonpath='{.items[0].spec.nodeName}')
t0=$(now_ms)
kubectl drain "$node" --ignore-daemonsets --delete-emptydir-data --force --timeout=120s
wait_state "$J4" SUCCEEDED 240
drain_recovery_ms=$(( $(now_ms) - t0 ))
summary=$("$BIN/gpuctl" status "$J4")
echo "$summary"
echo "$summary" | grep -q 'LOST' || fail "drained attempt was not recorded as LOST"
attempts=$(echo "$summary" | sed -n '1p' | grep -o 'attempt=[0-9]*/' | tr -dc 0-9)
[[ "$attempts" == 2 ]] || fail "expected the retry to be attempt 2, got $attempts"
"$BIN/gpuctl" pools
kubectl uncordon "$node"
record "{\"step\":\"node_drain\",\"node\":\"$node\",\"attempts\":$attempts,\"drain_to_success_ms\":$drain_recovery_ms}"

log "5. scheduler leader replacement"
before=$(admin leader)
leader=$(echo "$before" | python3 -c 'import json,sys; print(json.load(sys.stdin)["holder"])')
epoch=$(echo "$before" | python3 -c 'import json,sys; print(json.load(sys.stdin)["epoch"])')
t0=$(now_ms)
kubectl -n "$NS" delete pod "$leader" --grace-period=0 --force >/dev/null 2>&1
for _ in $(seq 1 120); do
  cur=$(admin leader 2>/dev/null || echo '{}')
  holder=$(echo "$cur" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("holder",""))')
  [[ -n "$holder" && "$holder" != "$leader" ]] && break
  sleep 0.5
done
takeover_ms=$(( $(now_ms) - t0 ))
[[ "$holder" != "$leader" ]] || fail "no new leader after deleting $leader"
new_epoch=$(echo "$cur" | python3 -c 'import json,sys; print(json.load(sys.stdin)["epoch"])')
J5=$(submit 1 sh -c 'echo new-leader')
wait_state "$J5" SUCCEEDED 120
record "{\"step\":\"leader_replacement\",\"old\":\"$leader\",\"new\":\"$holder\",\"epoch\":[${epoch},${new_epoch}],\"takeover_ms\":$takeover_ms}"

log "6. public status is read-only"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST 127.0.0.1:$STATUS_PORT/status.json)
[[ "$code" == 405 ]] || fail "status POST answered $code"
curl -sf 127.0.0.1:$STATUS_PORT/status.json | python3 -c '
import json,sys
d=json.load(sys.stdin)
allowed={"schema","generated_at","evidence","pools","queue"}
assert set(d)==allowed, set(d)
for p in d["pools"]:
    assert not any(k.endswith("_id") or k in ("tenant","user","command") for k in p), p
print("status keys ok:", sorted(d))'
record "{\"step\":\"status_read_only\",\"post_code\":$code}"

{
  printf '{"cluster":"kind","kind_version":"%s","k8s":"%s","results":[' "$(kind version | awk '{print $2}')" "$(kubectl version -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')"
  (IFS=,; printf '%s' "${RESULTS[*]}")
  printf ']}\n'
} > "$OUT"
log "PASS"
cat "$OUT"
