#!/bin/bash
# Integration test: ADD SHARD — provision the shard declared with
# provisioning = true while omni writes keep flowing, then cut over.
#
# Requires:
#   - local postgres at port 5432 with databases pgdog1, pgdog2, shard_0
#     (created by integration/setup.sh)
#   - wal_level = logical, max_replication_slots >= 8
#
# Leaves shard_0 wiped (it plays the empty new shard); rerun
# integration/setup.sh before suites or live unit tests that expect the
# standard test schema there.
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
PGDOG_USERS="${SCRIPT_DIR}/users.toml"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog

PGDOG_PORT=6499
PGDOG2_PORT=6500
PGDOG_PID=""
PGDOG2_PID=""
WRITER_PID=""

app() {
    psql "host=127.0.0.1 port=${PGDOG_PORT} dbname=pgdog user=pgdog password=pgdog" -tAc "$1"
}

app2() {
    psql "host=127.0.0.1 port=${PGDOG2_PORT} dbname=pgdog user=pgdog password=pgdog" -tAc "$1"
}

admin() {
    psql "host=127.0.0.1 port=${PGDOG_PORT} dbname=admin user=admin password=pgdog" -tAc "$1"
}

direct() {
    local db=$1 sql=$2
    psql -d "$db" -tAc "$sql"
}

cleanup() {
    if [ -n "${WRITER_PID}" ]; then
        kill ${WRITER_PID} 2>/dev/null || true
        wait ${WRITER_PID} 2>/dev/null || true
    fi
    if [ -n "${PGDOG2_PID}" ]; then
        kill ${PGDOG2_PID} 2>/dev/null || true
        wait ${PGDOG2_PID} 2>/dev/null || true
    fi
    if [ -n "${PGDOG_PID}" ]; then
        kill ${PGDOG_PID} 2>/dev/null || true
        wait ${PGDOG_PID} 2>/dev/null || true
    fi
    rm -f "${SCRIPT_DIR}/pgdog2.toml" "${SCRIPT_DIR}/pgdog_ddl.toml" "${SCRIPT_DIR}/pgdog_multi.toml"
}
trap cleanup EXIT

start_pgdog() {
    local cfg=${1:-${PGDOG_CONFIG}}
    ${PGDOG_BIN} --config "${cfg}" --users "${PGDOG_USERS}" &
    PGDOG_PID=$!
    # Wait for pgdog to accept connections.
    for _ in $(seq 1 50); do
        if app "SELECT 1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.2
    done
    echo "ERROR: pgdog did not start"
    exit 1
}

stop_pgdog() {
    if [ -n "${PGDOG_PID}" ]; then
        kill ${PGDOG_PID} 2>/dev/null || true
        wait ${PGDOG_PID} 2>/dev/null || true
        PGDOG_PID=""
    fi
}

# A second pgdog instance sharing the config, on its own port.
start_pgdog2() {
    sed "s/port = ${PGDOG_PORT}/port = ${PGDOG2_PORT}/" "${PGDOG_CONFIG}" > "${SCRIPT_DIR}/pgdog2.toml"
    ${PGDOG_BIN} --config "${SCRIPT_DIR}/pgdog2.toml" --users "${PGDOG_USERS}" &
    PGDOG2_PID=$!
    for _ in $(seq 1 50); do
        if app2 "SELECT 1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.2
    done
    echo "ERROR: second pgdog did not start"
    exit 1
}

stop_pgdog2() {
    if [ -n "${PGDOG2_PID}" ]; then
        kill ${PGDOG2_PID} 2>/dev/null || true
        wait ${PGDOG2_PID} 2>/dev/null || true
        PGDOG2_PID=""
    fi
}

# Wait until exactly N instances are live in the registry on pgdog1
# (rows from restarted processes stay "live" for the liveness window).
wait_for_fleet() {
    local expected=$1
    for _ in $(seq 1 60); do
        local live
        live=$(direct pgdog1 "SELECT count(*) FROM pgdog.instances \
            WHERE heartbeat_at > NOW() - INTERVAL '15 seconds'" 2>/dev/null || echo 0)
        if [ "${live}" = "${expected}" ]; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: fleet never reached ${expected} live instance(s)"
    return 1
}

# Wait until N instances registered on the new shard (their agents).
wait_for_agents() {
    local expected=$1
    for _ in $(seq 1 60); do
        local live
        live=$(direct shard_0 "SELECT count(*) FROM pgdog.instances \
            WHERE heartbeat_at > NOW() - INTERVAL '15 seconds'" 2>/dev/null || echo 0)
        if [ "${live}" -ge "${expected}" ] 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: agents never registered ${expected} instance(s) on the new shard"
    return 1
}

start_omni_writer() {
    (
        i=0
        while true; do
            i=$((i + 1))
            app "INSERT INTO orgs (id, shard_id) VALUES ('org_bg_${RANDOM}_${i}', 0) ON CONFLICT DO NOTHING" >/dev/null 2>&1 || true
            sleep 0.1
        done
    ) &
    WRITER_PID=$!
}

stop_omni_writer() {
    if [ -n "${WRITER_PID}" ]; then
        kill ${WRITER_PID} 2>/dev/null || true
        wait ${WRITER_PID} 2>/dev/null || true
        WRITER_PID=""
    fi
}

# wait_for_status TASK_ID STATUS TIMEOUT_SECS
wait_for_status() {
    local task_id=$1 status=$2 timeout=$3
    for _ in $(seq 1 $((timeout * 2))); do
        local current
        current=$(admin "SHOW TASKS" | grep "^${task_id}|" | cut -d'|' -f5 || true)
        if [ "${current}" = "${status}" ]; then
            return 0
        fi
        # Bail out early on task failure.
        local lifecycle
        lifecycle=$(admin "SHOW TASKS" | grep "^${task_id}|" | cut -d'|' -f4 || true)
        if [[ "${lifecycle}" == error* || "${lifecycle}" == failed* ]]; then
            echo "ERROR: task ${task_id} failed: $(admin "SHOW TASKS" | grep "^${task_id}|")"
            return 1
        fi
        sleep 0.5
    done
    echo "ERROR: task ${task_id} never reached status '${status}'"
    admin "SHOW TASKS" || true
    return 1
}

reset_all() {
    stop_omni_writer
    stop_pgdog2
    stop_pgdog
    psql -d pgdog1 -f "${SCRIPT_DIR}/init.sql" >/dev/null
    start_pgdog

    # Seed: two orgs pinned to shards 0 and 1, plus their data.
    app "INSERT INTO orgs (id, shard_id) VALUES ('org_zero', 0), ('org_one', 1)" >/dev/null
    app "INSERT INTO data (org_id, value) VALUES ('org_zero', 'a')" >/dev/null
    app "INSERT INTO data (org_id, value) VALUES ('org_one', 'b')" >/dev/null
}

assert_eq() {
    local left=$1 right=$2 message=$3
    if [ "${left}" != "${right}" ]; then
        echo "ASSERTION FAILED: ${message} (${left} != ${right})"
        exit 1
    fi
}

pushd "${SCRIPT_DIR}" >/dev/null

echo "=== Test A: parked cutover with live omni writes ==="
reset_all
start_omni_writer
sleep 1

TASK_ID=$(admin "ADD SHARD pgdog 2")
echo "task: ${TASK_ID}"
wait_for_status "${TASK_ID}" "awaiting cutover" 120

# The writer kept making progress while the task parked.
BEFORE=$(app "SELECT count(*) FROM orgs")
sleep 2
AFTER=$(app "SELECT count(*) FROM orgs")
if [ "${AFTER}" -le "${BEFORE}" ]; then
    echo "ASSERTION FAILED: omni writer stalled while task parked"
    exit 1
fi

admin "CUTOVER ${TASK_ID}" >/dev/null
wait_for_status "${TASK_ID}" "swapping topology" 60 || true
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "task finished"

stop_omni_writer
sleep 1

# Omni rows on the new shard match shard 0, including rows written
# during the sync.
SRC_COUNT=$(direct pgdog1 "SELECT count(*) FROM orgs")
NEW_COUNT=$(direct shard_0 "SELECT count(*) FROM orgs")
assert_eq "${SRC_COUNT}" "${NEW_COUNT}" "omni rows caught up on the new shard"

# No replication slots left behind.
SLOTS=$(direct pgdog1 "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%'")
assert_eq "${SLOTS}" "0" "replication slots cleaned up"

# A tenant assigned to the new shard routes there.
app "INSERT INTO orgs (id, shard_id) VALUES ('org_two', 2)" >/dev/null
app "INSERT INTO data (org_id, value) VALUES ('org_two', 'c')" >/dev/null
ROUTED=$(app "SELECT count(*) FROM data WHERE org_id = 'org_two'")
assert_eq "${ROUTED}" "1" "new tenant readable through pgdog"
ON_NEW=$(direct shard_0 "SELECT count(*) FROM data WHERE org_id = 'org_two'")
assert_eq "${ON_NEW}" "1" "new tenant's row landed on the new shard"

echo "=== Test B: automatic cutover ==="
reset_all
TASK_ID=$(admin "ADD SHARD pgdog 2 AUTO")
for _ in $(seq 1 240); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    if [[ "${LIFECYCLE}" == error* ]]; then
        echo "ERROR: auto add shard failed"
        admin "SHOW TASKS"
        exit 1
    fi
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "auto task finished"
SRC_COUNT=$(direct pgdog1 "SELECT count(*) FROM orgs")
NEW_COUNT=$(direct shard_0 "SELECT count(*) FROM orgs")
assert_eq "${SRC_COUNT}" "${NEW_COUNT}" "omni rows on the new shard (auto)"

echo "=== Test C: abort mid-task leaves topology unchanged ==="
reset_all
TASK_ID=$(admin "ADD SHARD pgdog 2")
admin "STOP_TASK ${TASK_ID}" >/dev/null
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    case "${LIFECYCLE}" in cancelled|finished) break ;; esac
    sleep 0.5
done

# Topology unchanged: an org pinned to shard 2 fails to route.
if app "INSERT INTO orgs (id, shard_id) VALUES ('org_never', 2)" >/dev/null 2>&1 \
    && app "INSERT INTO data (org_id, value) VALUES ('org_never', 'x')" >/dev/null 2>&1; then
    echo "ASSERTION FAILED: shard 2 should not exist after an aborted task"
    exit 1
fi

# Slots cleaned up after the abort.
sleep 2
SLOTS=$(direct pgdog1 "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%'")
assert_eq "${SLOTS}" "0" "replication slots cleaned up after abort"

echo "=== Test D: cross-instance lock refuses a second provisioner ==="
reset_all
# Simulate another pgdog instance holding the provisioning lock on the
# new shard: a session-scoped advisory lock, key ASCII "pgdog_ad".
psql -d shard_0 -tAc "SELECT pg_advisory_lock(8099552884487577956), pg_sleep(30)" >/dev/null 2>&1 &
LOCK_PID=$!
sleep 1
TASK_ID=$(admin "ADD SHARD pgdog 2")
LIFECYCLE=""
for _ in $(seq 1 60); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [[ "${LIFECYCLE}" == failed* ]] && break
    sleep 0.5
done
kill ${LOCK_PID} 2>/dev/null || true
wait ${LOCK_PID} 2>/dev/null || true
# Killing the client doesn't always end the server session holding the
# lock; terminate it so later tests can take the lock.
direct shard_0 "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
    WHERE query LIKE '%pg_advisory_lock%' AND pid != pg_backend_pid()" >/dev/null || true
if [[ "${LIFECYCLE}" != failed*provisioning* ]]; then
    echo "ASSERTION FAILED: task should fail while the lock is held (${LIFECYCLE})"
    exit 1
fi

echo "=== Test E: restart with a stale provisioning flag converges ==="
reset_all
TASK_ID=$(admin "ADD SHARD pgdog 2 AUTO")
for _ in $(seq 1 240); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "auto task finished before restart"

# Restart pgdog with the same config: it still declares shard 2 with
# provisioning = true (cutover_save_config is off, mimicking a config
# manifest that wasn't updated yet). The new shard reports itself live
# in its pgdog.config, so convergence must re-activate it.
stop_pgdog
start_pgdog

app "INSERT INTO orgs (id, shard_id) VALUES ('org_conv', 2)" >/dev/null
CONVERGED=""
for _ in $(seq 1 60); do
    if app "INSERT INTO data (org_id, value) VALUES ('org_conv', 'c')" >/dev/null 2>&1; then
        CONVERGED="yes"
        break
    fi
    sleep 0.5
done
assert_eq "${CONVERGED}" "yes" "restarted pgdog converged to 3 shards"
ON_NEW=$(direct shard_0 "SELECT count(*) FROM data WHERE org_id = 'org_conv'")
assert_eq "${ON_NEW}" "1" "post-convergence tenant landed on the new shard"

echo "=== Test F: two instances coordinate the cutover ==="
reset_all
start_pgdog2
wait_for_fleet 2
wait_for_agents 2

# The registry is operator-visible.
LIVE_ROWS=$(admin "SHOW INSTANCES" | grep -c "|t$" || true)
assert_eq "${LIVE_ROWS}" "2" "SHOW INSTANCES lists both live instances"

TASK_ID=$(admin "ADD SHARD pgdog 2")
wait_for_status "${TASK_ID}" "awaiting cutover" 120
admin "CUTOVER SHARD pgdog 2" >/dev/null
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "coordinated cutover finished"

# Both instances now route shard 2 tenants: the second one activated
# via the agent, without a reload.
app "INSERT INTO orgs (id, shard_id) VALUES ('org_f1', 2)" >/dev/null
app "INSERT INTO data (org_id, value) VALUES ('org_f1', 'f1')" >/dev/null
ROUTED2=""
for _ in $(seq 1 30); do
    if app2 "INSERT INTO data (org_id, value) VALUES ('org_f1', 'f2')" >/dev/null 2>&1; then
        ROUTED2="yes"
        break
    fi
    sleep 0.5
done
assert_eq "${ROUTED2}" "yes" "second instance activated the new shard"
ON_NEW=$(direct shard_0 "SELECT count(*) FROM data WHERE org_id = 'org_f1'")
assert_eq "${ON_NEW}" "2" "both instances routed to the new shard"

# Omni writes from the second instance reach all three shards.
app2 "INSERT INTO orgs (id, shard_id) VALUES ('org_f2', 0)" >/dev/null
OMNI_NEW=$(direct shard_0 "SELECT count(*) FROM orgs WHERE id = 'org_f2'")
assert_eq "${OMNI_NEW}" "1" "omni write from second instance reached the new shard"
stop_pgdog2

echo "=== Test G: a dead instance blocks the cutover until it expires ==="
reset_all
start_pgdog2
wait_for_fleet 2
wait_for_agents 2

TASK_ID=$(admin "ADD SHARD pgdog 2")
wait_for_status "${TASK_ID}" "awaiting cutover" 120

# Kill the second instance hard: its registry row stays "live" for the
# liveness window, so the cutover must refuse (arm acks never arrive)
# rather than diverge.
kill -9 ${PGDOG2_PID} 2>/dev/null || true
wait ${PGDOG2_PID} 2>/dev/null || true
PGDOG2_PID=""

admin "CUTOVER ${TASK_ID}" >/dev/null
sleep 12
STATUS=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f5 || true)
LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
if [ "${LIFECYCLE}" = "finished" ]; then
    echo "ASSERTION FAILED: cutover should not finish with a dead armed peer"
    exit 1
fi
assert_eq "${STATUS}" "awaiting cutover" "cutover re-parked while the dead peer looked live"

# Once the dead instance's heartbeat expires, the cutover proceeds solo.
sleep 16
admin "CUTOVER ${TASK_ID}" >/dev/null
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "cutover finished after the dead peer expired"

echo "=== Test H: schema-only add shard without omnisharded tables ==="
reset_all
# Same topology, but no omnisharded tables declared: the task syncs
# schema only and skips the copy, replication, and write pause.
stop_pgdog
sed '/\[\[omnisharded_tables\]\]/,+2d' "${PGDOG_CONFIG}" > "${SCRIPT_DIR}/pgdog_ddl.toml"
start_pgdog "${SCRIPT_DIR}/pgdog_ddl.toml"

TASK_ID=$(admin "ADD SHARD pgdog 2 AUTO")
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    if [[ "${LIFECYCLE}" == failed* ]]; then
        echo "ERROR: schema-only add shard failed"
        admin "SHOW TASKS"
        exit 1
    fi
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "schema-only task finished"

# The schema landed on the new shard.
HAS_ORGS=$(direct shard_0 "SELECT count(*) FROM pg_tables WHERE tablename = 'orgs'")
assert_eq "${HAS_ORGS}" "1" "schema restored on the new shard"

# The topology serves shard 2: a tenant pinned there routes correctly.
app "INSERT INTO orgs (id, shard_id) VALUES ('org_h', 2)" >/dev/null
app "INSERT INTO data (org_id, value) VALUES ('org_h', 'h')" >/dev/null
ON_NEW=$(direct shard_0 "SELECT count(*) FROM data WHERE org_id = 'org_h'")
assert_eq "${ON_NEW}" "1" "tenant routed to the schema-only shard"

# No replication slots were ever created.
SLOTS=$(direct pgdog1 "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%'")
assert_eq "${SLOTS}" "0" "no replication machinery in the schema-only path"

echo "=== Test I: several shards declared at once, commands name one ==="
reset_all
# Declare shard 3 alongside shard 2: future shards can pile up in the
# config, and each ADD SHARD names the one it works on.
stop_pgdog
cat "${PGDOG_CONFIG}" > "${SCRIPT_DIR}/pgdog_multi.toml"
cat >> "${SCRIPT_DIR}/pgdog_multi.toml" <<'EOF'

[[databases]]
name = "pgdog"
host = "127.0.0.1"
database_name = "shard_1"
shard = 3
provisioning = true
EOF
start_pgdog "${SCRIPT_DIR}/pgdog_multi.toml"

# Only the next shard can be added: shard 3 is refused while 2 waits.
TASK_ID=$(admin "ADD SHARD pgdog 3")
for _ in $(seq 1 60); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [[ "${LIFECYCLE}" == failed* ]] && break
    sleep 0.5
done
if [[ "${LIFECYCLE}" != failed*"next shard"* ]]; then
    echo "ASSERTION FAILED: shard 3 should be refused while 2 is pending (${LIFECYCLE})"
    exit 1
fi

# Shard 2, the next one, provisions and activates.
TASK_ID=$(admin "ADD SHARD pgdog 2")
wait_for_status "${TASK_ID}" "awaiting cutover" 120
admin "CUTOVER SHARD pgdog 2" >/dev/null
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [ "${LIFECYCLE}" = "finished" ] && break
    sleep 0.5
done
assert_eq "${LIFECYCLE}" "finished" "shard 2 added with shard 3 still declared"

app "INSERT INTO orgs (id, shard_id) VALUES ('org_i', 2)" >/dev/null
app "INSERT INTO data (org_id, value) VALUES ('org_i', 'i')" >/dev/null
ON_NEW=$(direct shard_0 "SELECT count(*) FROM data WHERE org_id = 'org_i'")
assert_eq "${ON_NEW}" "1" "tenant routed to shard 2 with shard 3 still declared"

popd >/dev/null
echo "add_shard integration tests passed"
