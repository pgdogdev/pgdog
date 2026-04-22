#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [[ "$OS" == "darwin" ]]; then
    ARCH=arm64
else
    ARCH=amd64
fi

# Verify and apply required PostgreSQL system settings.
# ALTER SYSTEM cannot run inside a function/DO block, so checks are done in bash.
# If any setting is changed the script exits — restart PostgreSQL and re-run.
_pg_needs_restart=false

_pg_check_int() {
    local name=$1 min=$2
    local current
    current=$(psql -tAc "SELECT current_setting('$name')::INTEGER")
    if [[ -z "$current" ]]; then
        echo "ERROR: could not read setting '$name'" >&2
        return 1
    fi
    if (( current < min )); then
        echo "Changing $name: $current -> $min"
        psql -c "ALTER SYSTEM SET $name TO $min"
        _pg_needs_restart=true
    fi
}

_pg_check_str() {
    local name=$1 expected=$2
    local current
    current=$(psql -tAc "SELECT current_setting('$name')")
    if [[ "$current" != "$expected" ]]; then
        echo "Changing $name: $current -> $expected"
        psql -c "ALTER SYSTEM SET $name TO '$expected'"
        _pg_needs_restart=true
    fi
}

_pg_check_int max_connections          1000
_pg_check_int max_prepared_transactions 1000
_pg_check_str wal_level                 logical
_pg_check_int max_worker_processes      64
_pg_check_int max_wal_senders           32
_pg_check_int max_replication_slots     32

if [[ "$_pg_needs_restart" == true ]]; then
    echo 'PostgreSQL settings changed. Restart PostgreSQL and re-run this script.'
    exit 1
fi

for user in pgdog pgdog1 pgdog2 pgdog3; do
    psql -c "DROP DATABASE ${user}" || true
    psql -c "DROP USER ${user}" || true
    psql -c "CREATE USER ${user} LOGIN SUPERUSER PASSWORD 'pgdog'" || true
    psql -c "CREATE DATABASE ${user}" || true
done

# GitHub fix
if [[ "$USER" == "runner" ]]; then
    psql -c "ALTER USER runner PASSWORD 'pgdog' LOGIN;"
fi

export PGPASSWORD='pgdog'
export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER='pgdog'

for db in pgdog shard_0 shard_1 shard_2 shard_3; do
    psql -c "DROP DATABASE $db" || true
    psql -c "CREATE DATABASE $db" || true
    for user in pgdog pgdog1 pgdog2 pgdog3; do
        psql -c "GRANT ALL ON DATABASE $db TO ${user}"
        psql -c "GRANT ALL ON SCHEMA public TO ${user}" ${db}
    done
done

for db in pgdog shard_0 shard_1 shard_2 shard_3; do
    for table in sharded sharded_omni; do
            psql -c "DROP TABLE IF EXISTS ${table}" ${db} -U pgdog
            psql -c "CREATE TABLE IF NOT EXISTS ${table} (
                id BIGINT PRIMARY KEY,
                value TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                enabled BOOLEAN DEFAULT false,
                user_id BIGINT,
                region_id INTEGER DEFAULT 10,
                country_id SMALLINT DEFAULT 5,
                options JSONB DEFAULT '{}'::jsonb
            )" ${db} -U pgdog
    done

    psql -c "CREATE TABLE IF NOT EXISTS sharded_varchar (id_varchar VARCHAR)" ${db} -U pgdog
    psql -c "CREATE TABLE IF NOT EXISTS sharded_uuid (id_uuid UUID PRIMARY KEY)" -d "$db" -U pgdog

    for table in list range; do
        psql -c "CREATE TABLE IF NOT EXISTS sharded_${table} (id BIGINT)" ${db} -U pgdog
    done

    psql -c "CREATE TABLE IF NOT EXISTS sharded_list_uuid (id_uuid UUID PRIMARY KEY)" -d "$db" -U pgdog

    psql -f ${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql ${db} -U ${user}
done

pushd ${SCRIPT_DIR}

for bin in toxiproxy-server toxiproxy-cli; do
    if [[ ! -f ${bin} ]]; then
        curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/${bin}-${OS}-${ARCH} > ${bin}
        chmod +x ${bin}
    fi
done

popd
