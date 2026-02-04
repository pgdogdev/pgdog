#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PGDOG_BIN_PATH="${PGDOG_BIN:-${SCRIPT_DIR}/../../target/debug/pgdog}"
pushd ${SCRIPT_DIR}

dropdb pgdog1 || true
dropdb pgdog2 || true
createdb pgdog1
createdb pgdog2

export PGPASSWORD=pgdog
export PGUSER=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432
psql -f ${SCRIPT_DIR}/ecommerce_schema.sql pgdog1
psql -c 'CREATE PUBLICATION pgdog FOR ALL TABLES' pgdog1 || true

${PGDOG_BIN_PATH} \
    schema-sync \
    --from-database source \
    --to-database destination \
    --publication pgdog

${PGDOG_BIN_PATH} \
    schema-sync \
    --from-database source \
    --to-database destination \
    --publication pgdog \
    --data-sync-complete

${PGDOG_BIN_PATH} \
    schema-sync \
    --from-database source \
    --to-database destination \
    --publication pgdog \
    --cutover

pg_dump \
    --schema-only \
    --exclude-schema pgdog \
    --no-publications pgdog1 > source.sql

pg_dump \
    --schema-only \
    --exclude-schema pgdog \
    --no-publications pgdog2 > destination.sql

for f in source.sql destination.sql; do
    sed -i.bak '/^\\restrict.*$/d' $f
    sed -i.bak '/^\\unrestrict.*$/d' $f
done

# Expected integer -> bigint conversions (normalized to just column_name and type)
# Format: column_name integer -> column_name bigint
EXPECTED_CONVERSIONS=$(cat <<EOF
audit_id integer
audit_id bigint
category_id integer
category_id bigint
document_id integer
document_id bigint
event_id integer
event_id bigint
flag_id integer
flag_id bigint
notification_id integer
notification_id bigint
override_id integer
override_id bigint
price_history_id integer
price_history_id bigint
session_id integer
session_id bigint
setting_id integer
setting_id bigint
ticket_id integer
ticket_id bigint
EOF
)

diff source.sql destination.sql > diff.txt || true

# Extract column name and type from diff lines, ignoring everything else
# This normalizes across different PG versions and constraint syntaxes
ACTUAL_CONVERSIONS=$(grep '^[<>]' diff.txt | \
    grep -E '\b(integer|bigint)\b' | \
    sed -E 's/.*[[:space:]]([a-z_]+)[[:space:]]+(integer|bigint).*/\1 \2/' | \
    sort -u)

EXPECTED_SORTED=$(echo "$EXPECTED_CONVERSIONS" | sort -u)

if [ "$ACTUAL_CONVERSIONS" != "$EXPECTED_SORTED" ]; then
    echo "Schema diff does not match expected integer -> bigint conversions"
    echo "=== Expected ==="
    echo "$EXPECTED_SORTED"
    echo "=== Actual ==="
    echo "$ACTUAL_CONVERSIONS"
    exit 1
fi

rm source.sql destination.sql diff.txt
popd
