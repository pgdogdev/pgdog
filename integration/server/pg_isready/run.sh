#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/../../common.sh"

export PGHOST=127.0.0.1 PGPORT=6432 PGDATABASE=pgdog PGSSLMODE=disable
export PGPASSFILE="${SCRIPT_DIR}/missing.pgpass" PGDOG_LOG_CONNECTIONS=true PGDOG_LOG_FORMAT=text RUST_LOG=info
unset PGPASSWORD

log="${COMMON_DIR}/log.txt"
run_pgdog integration/server 2>> "${log}"
pid=$!
if [[ -z "${pid}" ]]; then
    echo "This test needs its own PgDog process and log"
    exit 1
fi
wait_for_pgdog
for user in pgdog pgdog_missing; do
    pg_isready -U "${user}" -q
done

# Confirm that a real password failure is still logged.
if PGPASSWORD=wrong psql -Xw -U pgdog_migrator -c 'SELECT 1' > /dev/null 2>&1; then
    echo "PgDog accepted the wrong password"
    exit 1
fi

# Drain the login tasks before checking for warnings from the probes.
kill -TERM "${pid}"
wait "${pid}"
grep -Fq 'user "pgdog_migrator" and database "pgdog" auth error: wrong password' "${log}"
if grep -E 'user "(pgdog|pgdog_missing)" and database "pgdog" auth error:|client crashed:' "${log}"; then
    echo "pg_isready caused a login error"
    exit 1
fi
