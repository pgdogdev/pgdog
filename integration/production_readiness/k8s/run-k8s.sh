#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NAMESPACE="pgdog-test"
TENANT_COUNT="${1:-2000}"

echo "=== PgDog K8s Production Readiness Test ==="
echo "  Namespace: ${NAMESPACE}"
echo "  Tenants:   ${TENANT_COUNT}"
echo ""

kubectl apply -f namespace.yaml

kubectl apply -f postgres-statefulset.yaml
echo "Waiting for Postgres to be ready..."
kubectl -n "$NAMESPACE" rollout status statefulset/postgres --timeout=120s

# Patch tenant count if non-default
if [ "$TENANT_COUNT" != "2000" ]; then
  kubectl -n "$NAMESPACE" delete job tenant-setup --ignore-not-found
  cat tenant-setup-job.yaml \
    | sed "s/value: \"2000\"/value: \"${TENANT_COUNT}\"/" \
    | kubectl apply -f -
else
  kubectl -n "$NAMESPACE" delete job tenant-setup --ignore-not-found
  kubectl apply -f tenant-setup-job.yaml
fi

echo "Creating ${TENANT_COUNT} tenant databases (this may take a few minutes)..."
kubectl -n "$NAMESPACE" wait --for=condition=complete job/tenant-setup --timeout=600s

kubectl apply -f pgdog-configmap.yaml
kubectl apply -f pgdog-deployment.yaml
echo "Waiting for PgDog to be ready..."
kubectl -n "$NAMESPACE" rollout status deployment/pgdog --timeout=60s

# Port forward for local access (PgDog + Postgres)
echo ""
echo "Port forwarding PgDog and Postgres to localhost..."
kubectl -n "$NAMESPACE" port-forward svc/pgdog 6432:6432 9090:9090 &
PF_PGDOG=$!
kubectl -n "$NAMESPACE" port-forward svc/postgres 15432:5432 &
PF_PG=$!

# Give port-forward a moment to bind
sleep 2

echo ""
echo "=== PgDog is ready ==="
echo "  Proxy:    localhost:6432"
echo "  Metrics:  localhost:9090"
echo "  Postgres: localhost:15432 (direct)"
echo "  Tenants:  ${TENANT_COUNT}"
echo ""
echo "Test:    psql -h localhost -p 6432 -U pgdog -d tenant_1"
echo "Metrics: curl -s http://localhost:9090/metrics"
echo "Cleanup: kubectl delete namespace ${NAMESPACE}"
echo ""
echo "Press Ctrl+C to stop port forwarding"

cleanup_pf() {
  kill $PF_PGDOG 2>/dev/null || true
  kill $PF_PG 2>/dev/null || true
  exit 0
}
trap cleanup_pf INT TERM
wait $PF_PGDOG
