#!/bin/bash
# End-to-end test: pgdog connecting to Postgres over a Unix domain socket.
#
# Prerequisites:
#   - Postgres listening on a Unix socket (default: /tmp), trust or peer auth.
#   - pgdog built in the workspace (the script builds it if needed).
#
# Env overrides:
#   UNIX_SOCKET_DIR  - Postgres unix_socket_directories entry (default /tmp)
#   PG_PORT          - Postgres port (default 5432)
#   PROXY_PORT       - pgdog proxy listen port (default 6432)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOCKET_DIR="${UNIX_SOCKET_DIR:-/tmp}"
PG_PORT="${PG_PORT:-5432}"
PROXY_PORT="${PROXY_PORT:-6432}"

CONFIG="$(mktemp)"
PGOUT="$(mktemp)"
PGOOD_PID=""

cleanup() {
    if [[ -n "$PGOOD_PID" ]]; then
        kill "$PGOOD_PID" 2>/dev/null || true
        wait "$PGOOD_PID" 2>/dev/null || true
    fi
    rm -f "$CONFIG" "$PGOUT"
}
trap cleanup EXIT

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# --- 0. Preconditions ---------------------------------------------------------
if ! command -v pg_isready >/dev/null 2>&1; then
    fail "pg_isready not found (is Postgres client installed?)"
fi
if ! pg_isready -h "$SOCKET_DIR" -p "$PG_PORT" >/dev/null 2>&1; then
    fail "Postgres is not listening on unix socket $SOCKET_DIR:$PG_PORT"
fi
pass "Postgres is listening on unix socket $SOCKET_DIR (port $PG_PORT)"

# --- 1. Build pgdog -----------------------------------------------------------
echo "==> building pgdog"
cargo build --manifest-path "$ROOT/Cargo.toml" --bin pgdog

# --- 2. Config: backend pointed at the socket dir -----------------------------
cat > "$CONFIG" <<EOF
[general]
auth_type = "trust"

[[databases]]
name = "pgdog"
host = "$SOCKET_DIR"
port = $PG_PORT
database_name = "pgdog"
user = "pgdog"
EOF

# --- 3. Start pgdog -----------------------------------------------------------
echo "==> starting pgdog"
"$ROOT/target/debug/pgdog" --config "$CONFIG" --users "$ROOT/integration/users.toml" \
    >"$PGOUT" 2>&1 &
PGOOD_PID=$!

for _ in $(seq 1 30); do
    pg_isready -h 127.0.0.1 -p "$PROXY_PORT" -U pgdog -d pgdog >/dev/null 2>&1 && break
    sleep 1
done
if ! pg_isready -h 127.0.0.1 -p "$PROXY_PORT" -U pgdog -d pgdog >/dev/null 2>&1; then
    fail "pgdog did not become ready on 127.0.0.1:$PROXY_PORT"
fi
pass "pgdog is accepting connections on 127.0.0.1:$PROXY_PORT"

# --- 4. Query through pgdog ---------------------------------------------------
echo "==> querying through pgdog"
psql -h 127.0.0.1 -p "$PROXY_PORT" -U pgdog -d pgdog -v ON_ERROR_STOP=1 \
    -c "select version()" >/dev/null \
    || fail "query through pgdog failed"
pass "query through pgdog succeeded"

# --- 5. Server-side proof: unix socket connections have client_addr NULL ------
echo "==> backend connections as seen by Postgres"
CONNS="$(psql -h 127.0.0.1 -p "$PG_PORT" -U pgdog -d postgres -t -A \
    -c "select count(*) from pg_stat_activity where usename = 'pgdog' and backend_type = 'client backend' and client_addr is null")"
[[ "$CONNS" != "0" && -n "$CONNS" ]] || fail "no backend connections over unix socket (client_addr NULL)"
pass "$CONNS backend connection(s) over unix socket (client_addr IS NULL)"
psql -h 127.0.0.1 -p "$PG_PORT" -U pgdog -d postgres \
    -c "select pid, client_addr, client_hostname from pg_stat_activity where usename = 'pgdog' and backend_type = 'client backend'"

# --- 6. pgdog's own view of the backend address (best effort) -----------------
psql -h 127.0.0.1 -p "$PROXY_PORT" -U pgdog -d pgdog -c "SHOW SERVERS" \
    || echo "(note: SHOW SERVERS not available, skipping)"

echo
echo "ALL PASSED: pgdog -> Postgres over Unix domain socket"
