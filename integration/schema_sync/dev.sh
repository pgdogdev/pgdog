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
rm -f source.sql.bak destination.sql.bak

# Verify integer primary keys are rewritten to bigint, and no other differences exist
DIFF_OUTPUT=$(diff source.sql destination.sql || true)
echo "$DIFF_OUTPUT" | grep -q 'flag_id integer NOT NULL' || { echo "Expected flag_id integer->bigint rewrite"; exit 1; }
echo "$DIFF_OUTPUT" | grep -q 'flag_id bigint NOT NULL' || { echo "Expected flag_id integer->bigint rewrite"; exit 1; }
echo "$DIFF_OUTPUT" | grep -q 'setting_id integer NOT NULL' || { echo "Expected setting_id integer->bigint rewrite"; exit 1; }
echo "$DIFF_OUTPUT" | grep -q 'setting_id bigint NOT NULL' || { echo "Expected setting_id integer->bigint rewrite"; exit 1; }
sed -i.bak 's/flag_id integer NOT NULL/flag_id bigint NOT NULL/g' source.sql
sed -i.bak 's/setting_id integer NOT NULL/setting_id bigint NOT NULL/g' source.sql
rm -f source.sql.bak
diff source.sql destination.sql
rm source.sql
rm destination.sql
popd
