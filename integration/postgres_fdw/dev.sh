#!/bin/bash
set -ex -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGDOG="$SCRIPT_DIR/../../target/debug/pgdog"

dropdb shard_0_fdw || true
dropdb shard_1_fdw || true

createdb shard_0_fdw
createdb shard_1_fdw

psql -f "$SCRIPT_DIR/../schema_sync/ecommerce_schema.sql" shard_0_fdw
psql -f "$SCRIPT_DIR/../schema_sync/ecommerce_schema.sql" shard_1_fdw

${PGDOG}
