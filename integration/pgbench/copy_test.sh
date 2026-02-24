#!/bin/bash
#
# Integration test for COPY TO/FROM with binary and text protocols.
# Tests chunked data handling by copying 10,000 rows with various data types.
#
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PGPASSWORD=pgdog

# Connection parameters - can be overridden
HOST="${PGHOST:-127.0.0.1}"
PORT="${PGPORT:-6432}"
USER="${PGUSER:-pgdog}"
DB="${PGDATABASE:-pgdog}"

PSQL="psql -h $HOST -p $PORT -U $USER -d $DB -v ON_ERROR_STOP=1"

echo "=== COPY Integration Test ==="
echo "Connecting to $HOST:$PORT as $USER"

# Temp files for COPY data
TEXT_FILE=$(mktemp)
BINARY_FILE=$(mktemp)
trap "rm -f $TEXT_FILE $BINARY_FILE" EXIT

echo ""
echo "=== Setting up test table ==="

$PSQL <<'EOF'
-- Create uuid-ossp extension if not exists (for uuid_generate_v5)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS copy_test_types;

CREATE TABLE copy_test_types (
    id              SERIAL PRIMARY KEY,
    int_col         INTEGER NOT NULL,
    bigint_col      BIGINT NOT NULL,
    smallint_col    SMALLINT NOT NULL,
    real_col        REAL NOT NULL,
    double_col      DOUBLE PRECISION NOT NULL,
    numeric_col     NUMERIC(15, 4) NOT NULL,
    bool_col        BOOLEAN NOT NULL,
    char_col        CHAR(10) NOT NULL,
    varchar_col     VARCHAR(100) NOT NULL,
    text_col        TEXT NOT NULL,
    bytea_col       BYTEA NOT NULL,
    date_col        DATE NOT NULL,
    time_col        TIME NOT NULL,
    timetz_col      TIME WITH TIME ZONE NOT NULL,
    timestamp_col   TIMESTAMP NOT NULL,
    timestamptz_col TIMESTAMP WITH TIME ZONE NOT NULL,
    interval_col    INTERVAL NOT NULL,
    uuid_col        UUID NOT NULL,
    json_col        JSON NOT NULL,
    jsonb_col       JSONB NOT NULL,
    inet_col        INET NOT NULL,
    cidr_col        CIDR NOT NULL,
    macaddr_col     MACADDR NOT NULL,
    int_array       INTEGER[] NOT NULL,
    text_array      TEXT[] NOT NULL
);
EOF

echo "Table created with 25+ data types"

echo ""
echo "=== Generating 10,000 rows ==="

$PSQL <<'EOF'
INSERT INTO copy_test_types (
    int_col, bigint_col, smallint_col, real_col, double_col, numeric_col,
    bool_col, char_col, varchar_col, text_col, bytea_col,
    date_col, time_col, timetz_col, timestamp_col, timestamptz_col, interval_col,
    uuid_col, json_col, jsonb_col, inet_col, cidr_col, macaddr_col,
    int_array, text_array
)
SELECT
    i,                                                          -- int_col
    i * 1000000::BIGINT,                                        -- bigint_col
    (i % 32767)::SMALLINT,                                      -- smallint_col
    (i * 1.5)::REAL,                                            -- real_col
    (i * 2.5)::DOUBLE PRECISION,                                -- double_col
    (i * 3.1415)::NUMERIC(15, 4),                               -- numeric_col
    (i % 2 = 0),                                                -- bool_col
    LPAD(i::TEXT, 10, '0'),                                     -- char_col
    'varchar_' || i::TEXT,                                      -- varchar_col
    'This is row number ' || i::TEXT || ' with some text.',    -- text_col
    ('\x' || LPAD(TO_HEX(i), 8, '0'))::BYTEA,                   -- bytea_col
    DATE '2020-01-01' + (i % 1000),                             -- date_col
    TIME '00:00:00' + (i || ' seconds')::INTERVAL,              -- time_col
    TIMETZ '00:00:00+00' + (i || ' seconds')::INTERVAL,         -- timetz_col
    TIMESTAMP '2020-01-01 00:00:00' + (i || ' seconds')::INTERVAL, -- timestamp_col
    TIMESTAMPTZ '2020-01-01 00:00:00+00' + (i || ' seconds')::INTERVAL, -- timestamptz_col
    (i || ' days ' || (i % 24) || ' hours')::INTERVAL,          -- interval_col
    uuid_generate_v5(uuid_nil(), i::TEXT),                      -- uuid_col (deterministic)
    ('{"id": ' || i || ', "name": "item_' || i || '"}')::JSON,  -- json_col
    ('{"id": ' || i || ', "tags": ["a", "b"]}')::JSONB,         -- jsonb_col
    ('192.168.' || (i % 256) || '.' || ((i / 256) % 256))::INET, -- inet_col
    ('10.' || (i % 256) || '.0.0/24')::CIDR,                    -- cidr_col
    ('00:00:00:' || LPAD(TO_HEX((i / 65536) % 256), 2, '0') || ':' ||
                   LPAD(TO_HEX((i / 256) % 256), 2, '0') || ':' ||
                   LPAD(TO_HEX(i % 256), 2, '0'))::MACADDR,     -- macaddr_col
    ARRAY[i, i+1, i+2],                                         -- int_array
    ARRAY['tag_' || i, 'label_' || (i % 100)]                   -- text_array
FROM generate_series(1, 10000) AS i;
EOF

INITIAL_COUNT=$($PSQL -t -c "SELECT COUNT(*) FROM copy_test_types" | tr -d ' ')
echo "Inserted $INITIAL_COUNT rows"

# Calculate checksum of original data (using sum of hash values to avoid large strings)
ORIGINAL_CHECKSUM=$($PSQL -t -c "SELECT SUM(('x' || SUBSTR(MD5(copy_test_types::TEXT), 1, 8))::BIT(32)::BIGINT) FROM copy_test_types" | tr -d ' ')
echo "Original data checksum: $ORIGINAL_CHECKSUM"

echo ""
echo "=== Testing TEXT format COPY ==="

echo "COPY TO STDOUT (text)..."
$PSQL -c "COPY copy_test_types TO STDOUT" > "$TEXT_FILE"
TEXT_LINES=$(wc -l < "$TEXT_FILE")
TEXT_SIZE=$(stat -c%s "$TEXT_FILE" 2>/dev/null || stat -f%z "$TEXT_FILE")
echo "  Exported $TEXT_LINES lines, $TEXT_SIZE bytes"

echo "Truncating table..."
$PSQL -c "TRUNCATE copy_test_types RESTART IDENTITY"

echo "COPY FROM STDIN (text)..."
$PSQL -c "COPY copy_test_types FROM STDIN" < "$TEXT_FILE"

TEXT_COUNT=$($PSQL -t -c "SELECT COUNT(*) FROM copy_test_types" | tr -d ' ')
TEXT_CHECKSUM=$($PSQL -t -c "SELECT SUM(('x' || SUBSTR(MD5(copy_test_types::TEXT), 1, 8))::BIT(32)::BIGINT) FROM copy_test_types" | tr -d ' ')

echo "  Imported $TEXT_COUNT rows"
echo "  Checksum after TEXT round-trip: $TEXT_CHECKSUM"

if [ "$ORIGINAL_CHECKSUM" != "$TEXT_CHECKSUM" ]; then
    echo "ERROR: TEXT format checksum mismatch!"
    echo "  Expected: $ORIGINAL_CHECKSUM"
    echo "  Got:      $TEXT_CHECKSUM"
    exit 1
fi
echo "  TEXT format: PASSED"

echo ""
echo "=== Testing BINARY format COPY ==="

echo "COPY TO STDOUT (binary)..."
$PSQL -c "COPY copy_test_types TO STDOUT WITH (FORMAT binary)" > "$BINARY_FILE"
BINARY_SIZE=$(stat -c%s "$BINARY_FILE" 2>/dev/null || stat -f%z "$BINARY_FILE")
echo "  Exported $BINARY_SIZE bytes"

echo "Truncating table..."
$PSQL -c "TRUNCATE copy_test_types RESTART IDENTITY"

echo "COPY FROM STDIN (binary)..."
$PSQL -c "COPY copy_test_types FROM STDIN WITH (FORMAT binary)" < "$BINARY_FILE"

BINARY_COUNT=$($PSQL -t -c "SELECT COUNT(*) FROM copy_test_types" | tr -d ' ')
BINARY_CHECKSUM=$($PSQL -t -c "SELECT SUM(('x' || SUBSTR(MD5(copy_test_types::TEXT), 1, 8))::BIT(32)::BIGINT) FROM copy_test_types" | tr -d ' ')

echo "  Imported $BINARY_COUNT rows"
echo "  Checksum after BINARY round-trip: $BINARY_CHECKSUM"

if [ "$ORIGINAL_CHECKSUM" != "$BINARY_CHECKSUM" ]; then
    echo "ERROR: BINARY format checksum mismatch!"
    echo "  Expected: $ORIGINAL_CHECKSUM"
    echo "  Got:      $BINARY_CHECKSUM"
    exit 1
fi
echo "  BINARY format: PASSED"

echo ""
echo "=== Testing CSV format COPY ==="

CSV_FILE=$(mktemp)
trap "rm -f $TEXT_FILE $BINARY_FILE $CSV_FILE" EXIT

echo "COPY TO STDOUT (csv)..."
$PSQL -c "COPY copy_test_types TO STDOUT WITH (FORMAT csv, HEADER)" > "$CSV_FILE"
CSV_LINES=$(wc -l < "$CSV_FILE")
CSV_SIZE=$(stat -c%s "$CSV_FILE" 2>/dev/null || stat -f%z "$CSV_FILE")
echo "  Exported $CSV_LINES lines (including header), $CSV_SIZE bytes"

echo "Truncating table..."
$PSQL -c "TRUNCATE copy_test_types RESTART IDENTITY"

echo "COPY FROM STDIN (csv)..."
$PSQL -c "COPY copy_test_types FROM STDIN WITH (FORMAT csv, HEADER)" < "$CSV_FILE"

CSV_COUNT=$($PSQL -t -c "SELECT COUNT(*) FROM copy_test_types" | tr -d ' ')
CSV_CHECKSUM=$($PSQL -t -c "SELECT SUM(('x' || SUBSTR(MD5(copy_test_types::TEXT), 1, 8))::BIT(32)::BIGINT) FROM copy_test_types" | tr -d ' ')

echo "  Imported $CSV_COUNT rows"
echo "  Checksum after CSV round-trip: $CSV_CHECKSUM"

if [ "$ORIGINAL_CHECKSUM" != "$CSV_CHECKSUM" ]; then
    echo "ERROR: CSV format checksum mismatch!"
    echo "  Expected: $ORIGINAL_CHECKSUM"
    echo "  Got:      $CSV_CHECKSUM"
    exit 1
fi
echo "  CSV format: PASSED"

echo ""
echo "=== Cleanup ==="
$PSQL -c "DROP TABLE copy_test_types"

echo ""
echo "========================================"
echo "ALL COPY TESTS PASSED"
echo "========================================"
echo "  - TEXT format:   $TEXT_COUNT rows round-tripped"
echo "  - BINARY format: $BINARY_COUNT rows round-tripped"
echo "  - CSV format:    $CSV_COUNT rows round-tripped"
echo "  - All checksums match original data"
