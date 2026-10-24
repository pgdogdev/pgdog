#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

VAULT="docker compose -f ${SCRIPT_DIR}/docker-compose.yml exec -T -e VAULT_TOKEN=root vault vault"
STATIC_ROLE="pgdog-static-role"

export PGHOST=127.0.0.1
export PGPORT=6432

# Prints "<password> <ttl>" for the static role. Never echo the password itself.
static_creds() {
    $VAULT read -format=json "database/static-creds/${STATIC_ROLE}" | jq -r '"\(.data.password) \(.data.ttl)"'
}

# Connect through pgdog as the static role user with the given password.
static_psql() {
    local password="$1"
    shift
    PGUSER="pgdog_static" PGPASSWORD="${password}" psql -q -X pgdog "$@"
}

# A continuous stream of static role clients. Like a client talking to
# Postgres directly, a worker hitting a rejected password re-reads it from
# Vault and retries once. A failure after that is one pgdog introduced.
static_stream() {
    local worker="$1"
    while [ "$(date +%s)" -lt "$end" ]; do
        read -r password ttl <<< "$(static_creds)"
        if ! static_psql "${password}" -f "${SCRIPT_DIR}/test.sql" > /dev/null 2>&1; then
            read -r password ttl <<< "$(static_creds)"
            if ! static_psql "${password}" -f "${SCRIPT_DIR}/test.sql" > /dev/null; then
                echo "FAIL: static stream worker ${worker} failed after re-reading the password from Vault"
                return 1
            fi
        fi
    done
}

cleanup() {
    local workers
    workers=$(jobs -p)
    if [ -n "${workers}" ]; then
        kill ${workers} 2> /dev/null || true
    fi
    if [ -f "${COMMON_DIR}/pgdog.pid" ]; then
        stop_pgdog
    fi
}

run_pgdog integration/vault
trap cleanup EXIT
wait_for_pgdog

echo "Starting tests for static and dynamic roles..."
# Vault rotates the static role every 60s and dynamic leases expire after
# 120s at most; 180s covers ~3 rotations and a full lease turnover.
end=$(( $(date +%s) + 180 ))

PGUSER=pgdog PGPASSWORD=pgdog pgbench -n -f "${SCRIPT_DIR}/pgbench.sql" -P 10 pgdog -c 5 -T 180 &
pgbench_pid=$!

stream_pids=()
for worker in 1 2 3 4; do
    static_stream "${worker}" &
    stream_pids+=($!)
done
prev=""
rotations=0
while [ "$(date +%s)" -lt "$end" ]; do
    read -r password ttl <<< "$(static_creds)"
    # Rotation is imminent; pgdog's cache expires 1s before it, wait it out.
    if [ "${ttl}" -le 1 ]; then
        sleep 1
        continue
    fi

    if [ -n "${prev}" ] && [ "${password}" != "${prev}" ]; then
        rotations=$((rotations + 1))
        echo "Static password rotated (${rotations}), checking the previous one is rejected..."
        if static_psql "${prev}" -c 'SELECT 1' > /dev/null 2>&1; then
            echo "FAIL: pgdog accepted the previous static password after rotation"
            exit 1
        fi
    fi
    prev="${password}"

    static_psql "${password}" -f "${SCRIPT_DIR}/test.sql"
done

for pid in "${stream_pids[@]}"; do
    wait "${pid}"
done
echo "Static stream OK"

wait "${pgbench_pid}"
echo "Dynamic role OK"

if [ "${rotations}" -lt 1 ]; then
    echo "FAIL: expected Vault to rotate the static password at least once, saw ${rotations} rotations"
    exit 1
fi
echo "Static role OK (${rotations} rotations)"

echo "Starting test for early static role rotation..."
# Make sure pgdog's cached entries can't expire naturally during the test.
read -r password ttl <<< "$(static_creds)"
while [ "${ttl}" -le 20 ]; do
    sleep 1
    read -r password ttl <<< "$(static_creds)"
done
old_password="${password}"
static_psql "${old_password}" -c 'SELECT 1' > /dev/null

# Out-of-schedule rotation: pgdog only learns about it on RELOAD.
$VAULT write -f "database/rotate-role/${STATIC_ROLE}" > /dev/null
PGUSER=admin PGPASSWORD=pgdog psql -q -X admin -c 'RELOAD' > /dev/null
read -r password ttl <<< "$(static_creds)"

static_psql "${password}" -c 'SELECT 1' > /dev/null
if static_psql "${old_password}" -c 'SELECT 1' > /dev/null 2>&1; then
    echo "FAIL: pgdog accepted the previous static password after RELOAD"
    exit 1
fi
echo "Early rotation OK"
