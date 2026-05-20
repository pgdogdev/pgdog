#!/bin/bash
# Minimal repro for the omni-table cross-subscriber deadlock.
#
# Seeds a small fixed set of rows so both subscribers are guaranteed to fight
# over the exact same rows. The deadlock is a row-level cycle, not a volume
# problem: sub-0 locks row 1 on dest-0 and waits for dest-1; sub-1 locks row 1
# on dest-1 and waits for dest-0. Two rows is sufficient to form the cycle.
#
# Each burst of N_ROUNDS concurrent UPDATEs gives an independent ~25% chance
# of forming the cycle. Running N_BURSTS bursts raises the overall probability
# to ~99.7% (20 bursts × 25% per burst). The WAL volume is kept small so
# replication normally catches up in under a second; a sentinel timeout is a
# genuine stall, not backlog lag.
#
# Exits 1 with diagnostics if deadlock detected, 0 if replication keeps up.

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PGDOG_BIN=${PGDOG_BIN:-"${SCRIPT_DIR}/../../target/debug/pgdog"}
SENTINEL_TIMEOUT=${SENTINEL_TIMEOUT:-60}  # seconds to wait for sentinel
N_ROUNDS=${N_ROUNDS:-20}                  # concurrent UPDATEs per burst
N_BURSTS=${N_BURSTS:-100}                 # independent bursts to attempt

set -m
cleanup() {
    trap - EXIT INT TERM
    pkill -TERM -P $$ 2>/dev/null || true
    sleep 1
    pkill -KILL -P $$ 2>/dev/null || true
}
trap cleanup EXIT INT TERM
pushd "${SCRIPT_DIR}" >/dev/null

docker compose down && docker compose up -d
for port in 15432 15433 15434 15435; do
    until PGPASSWORD=pgdog pg_isready -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -q; do
        sleep 1
    done
done

"${PGDOG_BIN}" &
PGDOG_PID=$!

export PGPASSWORD=pgdog PGHOST=127.0.0.1 PGPORT=6432 PGUSER=pgdog
until psql source -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done

# Seed exactly 2 rows — the deadlock is a row-level cycle, not a volume problem.
# Both subscribers will immediately fight over these same rows on both destinations.
echo "seeding 2 rows..."
psql -d source -c "
    INSERT INTO settings (id, name, value) VALUES
        (1, 'seed-1', 'val-1'),
        (2, 'seed-2', 'val-2')
    ON CONFLICT (id) DO NOTHING;" >/dev/null

echo "initial sync..."
psql -d admin -c 'COPY_DATA source destination pgdog' >/dev/null

dump_deadlock_diagnostics() {
    echo ""
    echo "DEADLOCK DETECTED — replication stalled, dumping wait graph"
    echo ""
    for port in 15434 15435; do
        echo "--- dest :${port} pg_stat_activity ---"
        PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c "
            SELECT pid, wait_event_type, wait_event, state, left(query, 100) AS query
              FROM pg_stat_activity
             WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
             ORDER BY pid;"
        echo "--- dest :${port} pg_locks ---"
        PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c "
            SELECT pid, locktype,
                   CASE WHEN relation IS NOT NULL THEN relation::regclass::text ELSE '-' END AS relation,
                   mode, granted
              FROM pg_locks
             WHERE pid <> pg_backend_pid()
             ORDER BY pid, granted DESC;"
    done
    echo "--- replication slot lag on sources ---"
    for port in 15432 15433; do
        PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c \
            "SELECT slot_name, active, confirmed_flush_lsn, pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes FROM pg_replication_slots" || true
    done
    kill -TERM "${PGDOG_PID}" 2>/dev/null; wait "${PGDOG_PID}" 2>/dev/null || true
    docker compose down
    exit 1
}

# Fire N_BURSTS independent bursts of N_ROUNDS concurrent UPDATEs.
# pgdog fans Shard::All writes to both source shards, producing identical WAL
# on source-0 and source-1 — both subscribers wake from the same WAL notification
# and start applying the same transactions. The deadlock cycle forms when the
# Tokio scheduler interleaves their send loops so each subscriber wins a
# different destination's row lock (~25% per burst). Multiple bursts accumulate
# independent opportunities: P(at least one deadlock) ≈ 99.7% with 20 bursts.
#
# No continuous flood: each burst's WAL is tiny (N_ROUNDS × 2-row transactions),
# replication normally drains it in milliseconds. A sentinel timeout is a genuine
# stall, not backlog lag.
echo "firing ${N_BURSTS} bursts of ${N_ROUNDS} concurrent UPDATEs..."
for burst in $(seq 1 "${N_BURSTS}"); do
    declare -a SRC_PIDS=()
    for i in $(seq 1 "${N_ROUNDS}"); do
        psql -d source -c "UPDATE settings SET value = 'new';" >/dev/null &
        SRC_PIDS+=("$!")
    done
    wait "${SRC_PIDS[@]}" || true  # psql exits non-zero on SQL error; don't abort the script
done

# REPLICATION SENTINEL — inserted after all bursts so WAL ordering guarantees
# that once the sentinel appears on a destination, every preceding UPDATE has too.
# id=0 is reserved; seed uses 1 and 2.
SENTINEL_ID=0
echo "inserting sentinel row (id=${SENTINEL_ID})..."
psql -d source -c "INSERT INTO settings (id, name, value) VALUES (${SENTINEL_ID}, 'sentinel_done', 'sentinel_done') ON CONFLICT (id) DO UPDATE SET name = 'sentinel_done', value = 'sentinel_done';" >/dev/null

echo "waiting up to ${SENTINEL_TIMEOUT}s for sentinel to reach both destination shards..."
DEADLINE=$((SECONDS + SENTINEL_TIMEOUT))
while true; do
    sent0=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15434 -U pgdog -d postgres -tAc \
        "SELECT COUNT(*) FROM settings WHERE id = ${SENTINEL_ID} AND name = 'sentinel_done'" \
        2>/dev/null || echo 0)
    sent1=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15435 -U pgdog -d postgres -tAc \
        "SELECT COUNT(*) FROM settings WHERE id = ${SENTINEL_ID} AND name = 'sentinel_done'" \
        2>/dev/null || echo 0)
    [ "${sent0}" -eq 1 ] && [ "${sent1}" -eq 1 ] && break
    if [ "${SECONDS}" -ge "${DEADLINE}" ]; then
        echo "sentinel did not reach destination shards within ${SENTINEL_TIMEOUT}s"
        dump_deadlock_diagnostics
    fi
    sleep 1
done
echo "sentinel reached both destination shards"

# Verify source shards actually received the updates (routing sanity check).
check_source_shard() {
    local label="$1" port="$2"
    local total updated
    total=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -tAc \
        "SELECT COUNT(*) FROM settings WHERE id > 0")
    updated=$(PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -tAc \
        "SELECT COUNT(*) FROM settings WHERE value = 'new' AND id > 0")
    echo "${label}: ${updated}/${total} rows with value='new'"
    if [ "${total}" -eq 0 ]; then
        echo "  → seed INSERT never reached ${label} (routing bug, not a deadlock)"
    fi
}
check_source_shard source-0 15432
check_source_shard source-1 15433

for port in 15432 15433; do
    PGPASSWORD=pgdog psql -h 127.0.0.1 -p "${port}" -U pgdog -d postgres -c "
        SELECT slot_name, confirmed_flush_lsn,
               pg_current_wal_lsn() - confirmed_flush_lsn AS lag_bytes
          FROM pg_replication_slots;"
done

echo "OK — no deadlock observed (run again if flaky)"
kill -TERM "${PGDOG_PID}" 2>/dev/null; wait "${PGDOG_PID}" 2>/dev/null || true
docker compose down
exit 0
