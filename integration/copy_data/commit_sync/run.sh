#!/bin/bash
# COPY_DATA cross-shard atomic-commit test (3 shards).
#
# Verifies that data-sync's per-shard COPY is atomic and recovers from a transient shard
# failure. One shard's link is severed at the COPY commit handshake (its CopyDone), then
# healed while data-sync retries: the retry must re-copy cleanly and load every row. If
# the first attempt had left any shard partially committed, the retry would refuse the
# non-empty tables and data-sync would fail instead.
#
# Setup: source database (pgdog) sharded by tenant_id into three shards. The shards are
# the same Postgres on :5432 (databases shard_0/1/2), each reached through its own
# toxiproxy listener (15500/15501/15502) so one shard's link can be cut. Source
# data (setup.sql): three tenants x 2000 fixed-size rows, one tenant per shard.
#
# pgdog.toml options that matter here:
#   resharding_copy_format = "binary"     fixed-width rows, so the toxic can be sized by row count
#   resharding_parallel_copies = 1        one stream per shard, so the failure point is deterministic
#   resharding_copy_retry_*               high number of retries and slow delay to verify the rollback multiple times until toxic is active
#
# Steps:
#   1. Load source data and create the empty destination table on each shard.
#   2. Start toxiproxy with one proxy per shard.
#   3. Size a byte-limit toxic on shard 2 that severs its link at the CopyDone handshake.
#   4. Apply the toxic and run data-sync in the background; once the first attempt fails
#      and it enters its retry backoff, remove the toxic so the next retry reaches shard 2.
#   5. The retry must recover: data-sync exits cleanly and every shard holds all its rows
#      (2000/shard, 6000 total).
#
# Requires: Postgres on 5432 with databases pgdog/shard_0/shard_1/shard_2
#           (integration/setup.sh), integration/toxiproxy-{server,cli}, target/debug/pgdog.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
INTEGRATION_DIR=$( cd -- "${SCRIPT_DIR}/../.." &> /dev/null && pwd )
PGDOG_BIN=${PGDOG_BIN:-"${SCRIPT_DIR}/../../../target/debug/pgdog"}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
USERS_CONFIG="${SCRIPT_DIR}/users.toml"
TOXI_CLI="${INTEGRATION_DIR}/toxiproxy-cli"
TOXI_SERVER="${INTEGRATION_DIR}/toxiproxy-server"

export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=pgdog PGPASSWORD=pgdog

SLOT="commit_sync_slot"
SHARD0_PROXY="commit_sync_shard_0"
SHARD1_PROXY="commit_sync_shard_1"
SHARD2_PROXY="commit_sync_shard_2"
SHARD2_TENANT=1   # tenant routed to the fault-injected shard (see setup.sql)

src_psql()    { psql -d pgdog   -qX "$@"; }
shard0_psql() { psql -d shard_0 -qX "$@"; }
shard1_psql() { psql -d shard_1 -qX "$@"; }
shard2_psql() { psql -d shard_2 -qX "$@"; }
count()       { "$1" -tAc "SELECT count(*) FROM commit_sync.items"; }

drop_slots() {
    # Match the persistent slot data-sync creates ("<--replication-slot>_<shard>", e.g.
    # commit_sync_slot_0); temporary copy slots auto-drop on disconnect.
    src_psql -tAc "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '${SLOT}%' OR temporary" >/dev/null 2>&1 || true
}

cleanup() {
    # Killing toxiproxy-server drops all proxies and toxics (it holds them in memory).
    killall toxiproxy-server 2>/dev/null || true
    drop_slots
    # Tear down schema + publication on every database (see cleanup.sql) so nothing
    # (data, publication, or WAL-pinning slot) lingers.
    src_psql    -f "${SCRIPT_DIR}/cleanup.sql" >/dev/null 2>&1 || true
    shard0_psql -f "${SCRIPT_DIR}/cleanup.sql" >/dev/null 2>&1 || true
    shard1_psql -f "${SCRIPT_DIR}/cleanup.sql" >/dev/null 2>&1 || true
    shard2_psql -f "${SCRIPT_DIR}/cleanup.sql" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_data_sync() {
    "${PGDOG_BIN}" --config "${PGDOG_CONFIG}" --users "${USERS_CONFIG}" \
        data-sync --sync-only --skip-schema-sync \
        --from-database source --to-database destination \
        --publication commit_sync --replication-slot "${SLOT}"
}

# Step 1: load source schema + data, and create the destination table empty on each shard.
echo "[commit_sync] Step 1: loading source schema + data and empty destination tables..."
src_psql    -f "${SCRIPT_DIR}/setup.sql"
shard0_psql -f "${SCRIPT_DIR}/setup_schema.sql"
shard1_psql -f "${SCRIPT_DIR}/setup_schema.sql"
shard2_psql -f "${SCRIPT_DIR}/setup_schema.sql"

# Step 2: start toxiproxy and create one proxy per shard (one severable link each).
echo "[commit_sync] Step 2: starting toxiproxy and creating one proxy per shard..."
killall toxiproxy-server 2>/dev/null || true
"${TOXI_SERVER}" >/dev/null 2>&1 &
until "${TOXI_CLI}" list >/dev/null 2>&1; do sleep 0.2; done
"${TOXI_CLI}" create --listen "127.0.0.1:15500" --upstream "127.0.0.1:5432" "${SHARD0_PROXY}"
"${TOXI_CLI}" create --listen "127.0.0.1:15501" --upstream "127.0.0.1:5432" "${SHARD1_PROXY}"
"${TOXI_CLI}" create --listen "127.0.0.1:15502" --upstream "127.0.0.1:5432" "${SHARD2_PROXY}"

SRC=$(count src_psql)
EXPECTED=$(src_psql -tAc "SELECT count(*) FROM commit_sync.items WHERE tenant_id = ${SHARD2_TENANT}")
echo "[commit_sync] source rows: ${SRC} (expected ${EXPECTED} per shard)"

# Step 3: size the toxic to let every data byte through and sever only the trailing
# CopyDone, so the failure lands in copy_done() after shards 0/1 have committed.
#
# Bytes shard 2 receives, in order (CopyData framing = 1-byte tag + 4-byte length):
#   header  CopyData: 5 + 19 (PGCOPY signature 11 + flags 4 + extension 4)   = 24 B (Shard::All)
#   row     CopyData: 5 + 2 (field count) + 12 (id) + 12 (tenant_id)
#                     + 4 (payload length prefix) + PAYLOAD_LEN              = 35 + PAYLOAD_LEN
#   trailer CopyData: 5 + 2 (-1 terminator)                                  = 7 B  (Shard::All)
# then a 5-byte CopyDone. limit_data closes the link once this many upstream bytes have
# passed, so the streamed total drops the CopyDone (any value in [total, total+4] works).
PAYLOAD_LEN=512   # repeat('x', PAYLOAD_LEN) in setup.sql
ROW_WIRE=$(( 35 + PAYLOAD_LEN ))
HEADER_WIRE=24    # CopyData(5) + PGCOPY header(19), sent to every shard
TRAILER_WIRE=7    # CopyData(5) + -1 terminator(2), sent to every shard
LIMIT=$(( HEADER_WIRE + EXPECTED * ROW_WIRE + TRAILER_WIRE ))
echo "[commit_sync] Step 3: toxic limit=${LIMIT} bytes (header ${HEADER_WIRE} + ${EXPECTED} rows x ${ROW_WIRE} B + trailer ${TRAILER_WIRE}); severs the CopyDone"

# Step 4: sever shard 2 at its CopyDone, run data-sync in the background, then heal it while
# it is still retrying. The first attempt fails fast; with resharding_copy_retry_min_delay
# = 10ms (exponential backoff capped at 32x, ~320ms) and 100 max attempts, data-sync keeps
# retrying throughout the wait, rolling back each time. Holding the toxic for
# WAIT_BEFORE_HEAL seconds lets several attempts fail, then removing it lets a later retry pass.
WAIT_BEFORE_HEAL=${WAIT_BEFORE_HEAL:-5}
echo "[commit_sync] Step 4: severing shard 2 at its CopyDone, healing after ${WAIT_BEFORE_HEAL}s..."
"${TOXI_CLI}" toxic add --toxicName cut --type limit_data --upstream --attribute bytes="${LIMIT}" "${SHARD2_PROXY}"
drop_slots

run_data_sync &
SYNC_PID=$!

sleep "${WAIT_BEFORE_HEAL}"

# At heal time data-sync should still be running: the first attempt failed and it is
# backing off before the next retry. kill -0 sends no signal but succeeds only while the
# process exists; if it already exited, the copy finished before we healed and the shard-2
# fault was never exercised.
SYNC_IN_PROGRESS=1
kill -0 "${SYNC_PID}" 2>/dev/null || SYNC_IN_PROGRESS=0
"${TOXI_CLI}" toxic delete --toxicName cut "${SHARD2_PROXY}" 2>/dev/null || true

SYNC_EXIT_CODE=0
wait "${SYNC_PID}" || SYNC_EXIT_CODE=$?
echo "[commit_sync] Step 4: data-sync exited rc=${SYNC_EXIT_CODE} (sync_in_progress=${SYNC_IN_PROGRESS})"

# Step 5: the retry must recover. data-sync should exit cleanly and every shard should
# hold all of its rows. A first attempt that committed any shard would make the retry
# refuse the non-empty tables, so data-sync would exit non-zero instead.
D0=$(count shard0_psql); D1=$(count shard1_psql); D2=$(count shard2_psql)
TOTAL=$((D0 + D1 + D2))
echo "[commit_sync] Step 5: source=${SRC} shard_0=${D0} shard_1=${D1} shard_2=${D2} total=${TOTAL}"

if [ "${SYNC_IN_PROGRESS}" -eq 0 ]; then
    echo "[commit_sync] SKIP: copy finished before healing; the shard-2 fault was not exercised (check toxic sizing or lower WAIT_BEFORE_HEAL)."
    exit 1
fi
if [ "${SYNC_EXIT_CODE}" -ne 0 ]; then
    echo "[commit_sync] FAIL: data-sync did not recover on retry (rc=${SYNC_EXIT_CODE}); shards ${D0}/${D1}/${D2}."
    exit 1
fi
if [ "${SRC}" -ne "${TOTAL}" ] || [ "${D0}" -ne "${EXPECTED}" ] || [ "${D1}" -ne "${EXPECTED}" ] || [ "${D2}" -ne "${EXPECTED}" ]; then
    echo "[commit_sync] FAIL: row count mismatch (source=${SRC} total=${TOTAL}; split ${D0}/${D1}/${D2})"
    exit 1
fi

echo "[commit_sync] PASS: data-sync recovered on retry; all ${SRC} rows copied (split ${D0}/${D1}/${D2})."
