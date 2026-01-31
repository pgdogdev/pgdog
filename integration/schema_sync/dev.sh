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

# Expected content changes (without line numbers for portability)
EXPECTED_CHANGES=$(cat <<EOF
<     document_id integer NOT NULL,
>     document_id bigint NOT NULL,
<     event_id integer NOT NULL,
>     event_id bigint NOT NULL,
<     flag_id integer NOT NULL,
>     flag_id bigint NOT NULL,
<     override_id integer NOT NULL,
>     override_id bigint NOT NULL,
<     flag_id integer NOT NULL,
>     flag_id bigint NOT NULL,
<     notification_id integer NOT NULL,
>     notification_id bigint NOT NULL,
<     session_id integer NOT NULL,
>     session_id bigint NOT NULL,
<     session_id integer DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
>     session_id bigint DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
<     session_id integer DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
>     session_id bigint DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
<     session_id integer DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
>     session_id bigint DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
<     session_id integer DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
>     session_id bigint DEFAULT nextval('core.session_data_session_id_seq'::regclass) CONSTRAINT session_data_session_id_not_null NOT NULL,
<     setting_id integer NOT NULL,
>     setting_id bigint NOT NULL,
<     price_history_id integer NOT NULL,
>     price_history_id bigint NOT NULL,
<     category_id integer NOT NULL,
>     category_id bigint NOT NULL,
<     price_history_id integer DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
>     price_history_id bigint DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
<     category_id integer CONSTRAINT price_history_category_id_not_null NOT NULL,
>     category_id bigint CONSTRAINT price_history_category_id_not_null NOT NULL,
<     price_history_id integer DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
>     price_history_id bigint DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
<     category_id integer CONSTRAINT price_history_category_id_not_null NOT NULL,
>     category_id bigint CONSTRAINT price_history_category_id_not_null NOT NULL,
<     price_history_id integer DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
>     price_history_id bigint DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
<     category_id integer CONSTRAINT price_history_category_id_not_null NOT NULL,
>     category_id bigint CONSTRAINT price_history_category_id_not_null NOT NULL,
<     price_history_id integer DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
>     price_history_id bigint DEFAULT nextval('inventory.price_history_price_history_id_seq'::regclass) CONSTRAINT price_history_price_history_id_not_null NOT NULL,
<     category_id integer CONSTRAINT price_history_category_id_not_null NOT NULL,
>     category_id bigint CONSTRAINT price_history_category_id_not_null NOT NULL,
<     ticket_id integer NOT NULL,
>     ticket_id bigint NOT NULL,
<     ticket_id integer DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
>     ticket_id bigint DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
<     ticket_id integer DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
>     ticket_id bigint DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
<     ticket_id integer DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
>     ticket_id bigint DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
<     ticket_id integer DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
>     ticket_id bigint DEFAULT nextval('sales.ticket_queue_ticket_id_seq'::regclass) CONSTRAINT ticket_queue_ticket_id_not_null NOT NULL,
< ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_0_100 FOR VALUES FROM (0) TO (100);
> ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_0_100 FOR VALUES FROM ('0') TO ('100');
< ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_100_200 FOR VALUES FROM (100) TO (200);
> ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_100_200 FOR VALUES FROM ('100') TO ('200');
< ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_200_300 FOR VALUES FROM (200) TO (300);
> ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_200_300 FOR VALUES FROM ('200') TO ('300');
< ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_300_plus FOR VALUES FROM (300) TO (MAXVALUE);
> ALTER TABLE ONLY inventory.price_history ATTACH PARTITION inventory.price_history_cat_300_plus FOR VALUES FROM ('300') TO (MAXVALUE);
EOF
)

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
