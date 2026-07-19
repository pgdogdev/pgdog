#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

# Force backend Postgres to require TLS in GitHub CI so we exercise the
# client<->PgDog<->Postgres path end to end. Skipped locally because dev
# clusters aren't guaranteed to have server certs configured.
if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    psql -c "ALTER SYSTEM SET ssl TO on"
    PSQL_VERSION=$(psql -tAc "SELECT current_setting('server_version_num')::int / 10000")
    sudo pg_ctlcluster "${PSQL_VERSION}" main restart
fi

run_pgdog integration/tls

# psql requires private keys to be 0600 (git doesn't preserve this).
chmod 600 ${SCRIPT_DIR}/*.key

# pg_isready doesn't present a client cert, so use run_psql from dev.sh instead.
source ${SCRIPT_DIR}/dev.sh --source-only

echo "Waiting for PgDog"
PID_FILE="${SCRIPT_DIR}/../pgdog.pid"
PID=""
if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
fi
while ! run_psql tls_user_a client; do
    if [ -n "${PID}" ] && ! kill -0 "${PID}" 2> /dev/null; then
        echo "PgDog process (pid ${PID}) exited before becoming ready"
        exit 1
    fi
    sleep 0.1
done
echo "PgDog is ready"

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
