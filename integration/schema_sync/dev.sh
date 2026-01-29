#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PGDOG_BIN_PATH="${PGDOG_BIN:-${SCRIPT_DIR}/../../target/release/pgdog}"
pushd ${SCRIPT_DIR}

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

# Expected content changes (without line numbers for portability)
EXPECTED_CHANGES=$(cat <<EOF
<     flag_id integer NOT NULL,
>     flag_id bigint NOT NULL,
<     setting_id integer NOT NULL,
>     setting_id bigint NOT NULL,
<     override_id integer NOT NULL,
>     override_id bigint NOT NULL,
<     flag_id integer NOT NULL,
>     flag_id bigint NOT NULL,
EOF)

diff source.sql destination.sql > diff.txt || true

# Extract just the content lines (< and >) for comparison
ACTUAL_CHANGES=$(grep '^[<>]' diff.txt)
if [ "$ACTUAL_CHANGES" != "$EXPECTED_CHANGES" ]; then
    echo "Schema diff does not match expected changes"
    echo "=== Expected ==="
    echo "$EXPECTED_CHANGES"
    echo "=== Actual ==="
    echo "$ACTUAL_CHANGES"
    exit 1
fi

rm source.sql destination.sql diff.txt
popd
