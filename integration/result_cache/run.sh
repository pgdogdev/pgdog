#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$(dirname "${BASH_SOURCE[0]}")"

docker compose up -d

cleanup() {
  docker compose down -v
}
trap cleanup EXIT

# Build PgDog.
cd "$ROOT"
cargo build -q -p pgdog --release

# Start PgDog pointed at this integration config.
./target/release/pgdog --config integration/result_cache/pgdog.toml --users integration/result_cache/users.toml &
PGDOG_PID=$!
trap "kill ${PGDOG_PID} 2>/dev/null || true; cleanup" EXIT

sleep 2

# Prime DB
PGPASSWORD=postgres psql -h 127.0.0.1 -p 5432 -U postgres -d pgdog -c "CREATE TABLE IF NOT EXISTS t (id int primary key, v int); INSERT INTO t (id,v) VALUES (1,1) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;"

# First SELECT should be miss (populates cache), second should be hit.
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres -d pgdog -c "SELECT v FROM t WHERE id = 1;"
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres -d pgdog -c "SELECT v FROM t WHERE id = 1;"

# Write invalidates (generation bump) and next SELECT should be miss again.
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres -d pgdog -c "UPDATE t SET v = 2 WHERE id = 1;"
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres -d pgdog -c "SELECT v FROM t WHERE id = 1;"

echo "OK"

