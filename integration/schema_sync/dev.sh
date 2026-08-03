#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PGDOG_BIN_PATH="${PGDOG_BIN:-${SCRIPT_DIR}/../../target/debug/pgdog}"
pushd ${SCRIPT_DIR}

SOURCE_SHARD=pgdog1_shard_0
DESTINATION_SHARDS=(pgdog2_shard_0 pgdog2_shard_1 pgdog2_shard_2)

bash ${SCRIPT_DIR}/prepare.sh

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

dump_schema() {
    pg_dump \
        --schema-only \
        --exclude-schema pgdog \
        --no-publications "$1" > "$2"

    sed -i.bak '/^\\restrict.*$/d' "$2"
    sed -i.bak '/^\\unrestrict.*$/d' "$2"
}

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

EXPECTED_SORTED=$(echo "$EXPECTED_CONVERSIONS" | sort -u)

dump_schema "${SOURCE_SHARD}" source.sql

for shard in "${DESTINATION_SHARDS[@]}"; do
    dump_schema "${shard}" destination.sql

    diff source.sql destination.sql > diff.txt || true

    REMOVED_INT=$(grep '^<' diff.txt | \
        sed -E 's/.*[[:space:]]([a-z_]+)[[:space:]]+integer\b.*/\1/' | \
        grep -E '^[a-z_]+$' | sort -u)

    ADDED_BIGINT=$(grep '^>' diff.txt | \
        sed -E 's/.*[[:space:]]([a-z_]+)[[:space:]]+bigint\b.*/\1/' | \
        grep -E '^[a-z_]+$' | sort -u)

    CONVERTED=$(comm -12 <(echo "$REMOVED_INT") <(echo "$ADDED_BIGINT"))

    ACTUAL_CONVERSIONS=$(echo "$CONVERTED" | while read col; do
        echo "$col integer"
        echo "$col bigint"
    done | sort -u)

    if [ "$ACTUAL_CONVERSIONS" != "$EXPECTED_SORTED" ]; then
        echo "Schema diff on ${shard} does not match expected integer -> bigint conversions"
        echo "=== Expected ==="
        echo "$EXPECTED_SORTED"
        echo "=== Actual ==="
        echo "$ACTUAL_CONVERSIONS"
        exit 1
    fi

    echo "${SOURCE_SHARD} -> ${shard}: schema matches"
done

rm -f source.sql destination.sql diff.txt source.sql.bak destination.sql.bak
popd
