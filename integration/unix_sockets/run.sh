#!/bin/bash
# End-to-end test: pgdog connecting to Postgres over a Unix domain socket.
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

export PGPASSWORD=pgdog
CTL_PSQL=(psql -h 127.0.0.1 -p 5432 -U pgdog -d postgres -t -A)

# Detect socket dir + port from the running Postgres.
SOCKET_DIR=$("${CTL_PSQL[@]}" -c "show unix_socket_directories" | cut -d, -f1)
PG_BACKEND_PORT=$("${CTL_PSQL[@]}" -c "show port")
if [ -z "${SOCKET_DIR}" ]; then
    echo "FAIL: could not detect unix_socket_directories from Postgres" >&2
    exit 1
fi
echo "Postgres unix socket directory: ${SOCKET_DIR} (port ${PG_BACKEND_PORT})"

# Pre-flight: pgdog must be able to reach Postgres over the socket as pgdog.
if ! psql -h "${SOCKET_DIR}" -p "${PG_BACKEND_PORT}" -U pgdog -d postgres \
    -c "select 1" >/dev/null 2>&1; then
    echo "FAIL: cannot connect to Postgres over unix socket ${SOCKET_DIR} as pgdog." >&2
    echo "      Check pg_hba.conf 'local' lines: trust, or peer with OS user pgdog." >&2
    exit 1
fi

# Patch the static config with the detected socket dir/port into a temp dir.
TMP_CFG_DIR=$(mktemp -d /tmp/pgdog-unix-cfg.XXXXXX)
sed -e "s|^host = .*|host = \"${SOCKET_DIR}\"|" \
    -e "s|^port = .*|port = ${PG_BACKEND_PORT}|" \
    "${SCRIPT_DIR}/pgdog.toml" > "${TMP_CFG_DIR}/pgdog.toml"
cp "${SCRIPT_DIR}/users.toml" "${TMP_CFG_DIR}/"

run_pgdog "${TMP_CFG_DIR}"
wait_for_pgdog

# 1. Query through the proxy.
psql -h 127.0.0.1 -p 6432 -U pgdog -d pgdog -v ON_ERROR_STOP=1 \
    -c "select version()" >/dev/null
echo "PASS: query through pgdog"

# 2. Backend connections over the Unix socket have client_addr IS NULL.
CONNS=$("${CTL_PSQL[@]}" -c \
    "select count(*) from pg_stat_activity where usename = 'pgdog' and backend_type = 'client backend' and client_addr is null")
if [ -z "${CONNS}" ] || [ "${CONNS}" = "0" ]; then
    echo "FAIL: no backend connections over Unix socket (client_addr IS NULL)" >&2
    exit 1
fi
echo "PASS: ${CONNS} backend connection(s) over Unix socket"

stop_pgdog
rm -rf "${TMP_CFG_DIR}"
