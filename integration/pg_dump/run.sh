#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

export PGUSER=pgdog
export PGPASSWORD=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432

SHARDS=(pg_dump_0 pg_dump_1)
RESTORE_DB=pg_dump_restore
DUMP_FILE="${SCRIPT_DIR}/dump.sql"
SCHEMA_LOAD_WAIT=2

fail() {
    echo "FAIL: $*"
    exit 1
}

query_one() {
    psql -X -v ON_ERROR_STOP=1 -tA -d "$1" -c "$2"
}

query_shards() {
    for db in "${SHARDS[@]}"; do
        query_one "${db}" "$1"
    done
}

expect_equal() {
    local name="$1" expected="$2" actual="$3"
    if [ "${expected}" != "${actual}" ]; then
        fail "${name}: expected '${expected}', got '${actual}'"
    fi
    echo "OK ${name}"
}

expect_restored() {
    expect_equal "$1" "$(query_one "${SHARDS[0]}" "$2")" "$(query_one "${RESTORE_DB}" "$2")"
}

expect_restored_from_shards() {
    expect_equal "$1" "$(query_shards "$2")" "$(query_one "${RESTORE_DB}" "$2")"
}

PGDOG_RUNNING=0

cleanup() {
    local status=$?
    if [ "${PGDOG_RUNNING}" = "1" ]; then
        stop_pgdog
    fi
    for db in "${SHARDS[@]}" "${RESTORE_DB}"; do
        dropdb --if-exists --force "${db}" || true
    done
    if [ "${status}" = "0" ]; then
        rm -f "${DUMP_FILE}"
    fi
}
export PGDOG_STOP_TRAP=1
trap cleanup EXIT

for db in "${SHARDS[@]}" "${RESTORE_DB}"; do
    dropdb --if-exists --force "${db}"
    createdb "${db}"
done

TENANT_ID=1
ORDER_COUNT=100
for db in "${SHARDS[@]}"; do
    psql -X -v ON_ERROR_STOP=1 -q -d "${db}" -f "${SCRIPT_DIR}/schema.sql"
    psql -X -v ON_ERROR_STOP=1 -q -d "${db}" \
        -v tenant_id=${TENANT_ID} -v order_count=${ORDER_COUNT} \
        -f "${SCRIPT_DIR}/seed.sql"
    TENANT_ID=$((TENANT_ID + 1))
    ORDER_COUNT=$((ORDER_COUNT + 50))
done

SQL_VIEW_OID="SELECT 'paid_orders'::regclass::oid"
if [ "$(query_one pg_dump_0 "${SQL_VIEW_OID}")" = "$(query_one pg_dump_1 "${SQL_VIEW_OID}")" ]; then
    fail "catalog OIDs are the same on both shards, the test cannot detect wrong-shard catalog reads"
fi

run_pgdog "integration/pg_dump"
PGDOG_RUNNING=1
wait_for_pgdog
sleep "${SCHEMA_LOAD_WAIT}"

pg_dump -h 127.0.0.1 -p 6432 -d pg_dump_sharded -f "${DUMP_FILE}"

ORDERS_ON_SHARDS=0
for count in $(query_shards "SELECT count(*) FROM orders"); do
    ORDERS_ON_SHARDS=$((ORDERS_ON_SHARDS + count))
done
expect_equal "unqualified COPY of sharded table" \
    "${ORDERS_ON_SHARDS}" \
    "$(psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 6432 -d pg_dump_sharded \
        -c "COPY orders TO STDOUT" | grep -c '')"

stop_pgdog
PGDOG_RUNNING=0

if grep -qF '???' "${DUMP_FILE}"; then
    fail "dump contains unresolved type names (???)"
fi
echo "OK dump has no unresolved type names"

psql -X -v ON_ERROR_STOP=1 -q -d "${RESTORE_DB}" -f "${DUMP_FILE}" > /dev/null
echo "OK dump restored"

expect_restored_from_shards "sharded table rows" \
    "SELECT tenant_id, count(*), sum(amount) FROM orders GROUP BY tenant_id ORDER BY tenant_id"

expect_restored "omnisharded table rows" \
    "SELECT string_agg(id || ':' || code, ',' ORDER BY id) FROM countries"

expect_restored "sticky omnisharded table rows" \
    "SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM migrations"

expect_restored "unconfigured table rows" \
    "SELECT string_agg(id || ':' || email, ',' ORDER BY id) FROM users"

expect_restored_from_shards "view rows" \
    "SELECT tenant_id, count(*), sum(amount) FROM paid_orders GROUP BY tenant_id ORDER BY tenant_id"

expect_restored "view definition" \
    "SELECT pg_get_viewdef('paid_orders'::regclass, true)"

expect_restored "enum column type" \
    "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'orders'::regclass AND attname = 'status'"

expect_restored "function arguments" \
    "SELECT pg_get_function_identity_arguments('tenant_total'::regproc)"

expect_restored "index definition" \
    "SELECT pg_get_indexdef('orders_status_idx'::regclass)"

echo "=== [pg_dump] all checks passed ==="
