#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CLI="${SCRIPT_DIR}/../toxiproxy-cli"

# Create the per-shard table so cross-shard transactions have something
# to write. Routed by id: 0 -> shard_0, 1 -> shard_1 (matches the
# sharded_mappings in pgdog.toml).
export PGPASSWORD=pgdog
psql -h 127.0.0.1 -p 5432 -U pgdog -d shard_0 \
    -c 'CREATE TABLE IF NOT EXISTS crash_safety_test (id BIGINT PRIMARY KEY, value TEXT)'
psql -h 127.0.0.1 -p 5432 -U pgdog -d shard_1 \
    -c 'CREATE TABLE IF NOT EXISTS crash_safety_test (id BIGINT PRIMARY KEY, value TEXT)'

# Toxiproxy: shard_1's traffic flows through :5435 so the spec can
# inject a timeout toxic and catch pgdog mid-2PC deterministically.
killall toxiproxy-server > /dev/null 2>&1 || true
"${SCRIPT_DIR}/../toxiproxy-server" > /dev/null &
sleep 1

"${CLI}" delete crash_safety_shard_1 > /dev/null 2>&1 || true
"${CLI}" create --listen :5435 --upstream 127.0.0.1:5432 crash_safety_shard_1
