#!/bin/bash
# End-to-end test: pgdog connecting to Postgres over a Unix domain socket.
#
# Prerequisites:
#   - Postgres listening on a Unix socket (default: /tmp), trust or peer auth.
#
# Verifies:
#   - pgdog's backend connections reach Postgres over the socket:
#      pg_stat_activity shows client_addr IS NULL for them.
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog $SCRIPT_DIR
wait_for_pgdog


CONNS=$(psql -h 127.0.0.1 -p 5432 -U pgdog -d postgres -t -A -c \
    "select count(*) from pg_stat_activity where usename = 'pgdog' and backend_type = 'client backend' and client_addr is null")
if [ -z "${CONNS}" ] || [ "${CONNS}" = "0" ]; then
    echo "FAIL: no backend connections over Unix socket (client_addr IS NULL)" >&2
    exit 1
fi
echo "PASS: ${CONNS} backend connection(s) over Unix socket"

stop_pgdog
