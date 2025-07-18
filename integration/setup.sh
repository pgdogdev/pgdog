#!/bin/bash
set -e

echo "=== Starting setup script ==="

export PGUSER=pgdog
export PGPASSWORD=pgdog
export PGHOST=127.0.0.1
export PGPORT=5432

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "SCRIPT_DIR=$SCRIPT_DIR"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
echo "Detected OS: $OS"

if [[ "$OS" == "darwin" ]]; then
    ARCH=arm64
else
    ARCH=amd64
fi
echo "Using ARCH: $ARCH"

echo "---- Creating users ----"
for user in pgdog pgdog1 pgdog2 pgdog3; do
    echo "Creating user $user (if not exists)"
    psql -c "CREATE USER ${user} LOGIN SUPERUSER PASSWORD 'pgdog'" || echo "User $user already exists" -U postgres
done

echo "---- GitHub runner fix ----"
if [[ "$USER" == "runner" ]]; then
    echo "Altering runner user password"
    psql -c "ALTER USER runner PASSWORD 'pgdog' LOGIN;" -U postgres
else
    echo "Skipping runner fix for user $USER"
fi

export PGPASSWORD='pgdog'
export PGHOST=127.0.0.1
export PGPORT=5432

echo "---- Creating databases and granting permissions ----"
for db in pgdog shard_0 shard_1; do
    echo "Creating database $db"
    psql -c "CREATE DATABASE $db" || echo "Database $db already exists" -U postgres
    for user in pgdog pgdog1 pgdog2 pgdog3; do
        echo "Granting all on database $db to $user"
        psql -c "GRANT ALL ON DATABASE $db TO ${user}"
        echo "Granting all on schema public in $db to $user"
        psql -c "GRANT ALL ON SCHEMA public TO ${user}" ${db}
    done
done

echo "---- Creating tables ----"
for db in pgdog shard_0 shard_1; do
    echo "Processing database $db"
    for table in sharded sharded_omni; do
        echo "Dropping table $table if exists"
        psql -c "DROP TABLE IF EXISTS ${table}" ${db} -U pgdog
        echo "Creating table $table"
        psql -c "CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, value TEXT)" ${db} -U pgdog
    done

    echo "Creating sharded_varchar"
    psql -c "CREATE TABLE IF NOT EXISTS sharded_varchar (id_varchar VARCHAR)" ${db} -U pgdog

    echo "Creating sharded_uuid"
    psql -c "CREATE TABLE IF NOT EXISTS sharded_uuid (id_uuid UUID PRIMARY KEY)" -d "$db" -U pgdog

    for table in list range; do
        echo "Creating sharded_$table"
        psql -c "CREATE TABLE IF NOT EXISTS sharded_${table} (id BIGINT)" ${db} -U pgdog
    done

    echo "Creating sharded_list_uuid"
    psql -c "CREATE TABLE IF NOT EXISTS sharded_list_uuid (id_uuid UUID PRIMARY KEY)" -d "$db" -U pgdog

    echo "Applying schema setup.sql to $db"
    psql -f ${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql ${db} -U pgdog || echo "Failed running setup.sql on $db"
done

echo "---- Downloading Toxiproxy binaries ----"
pushd ${SCRIPT_DIR} > /dev/null
for bin in toxiproxy-server toxiproxy-cli; do
    if [[ ! -f ${bin} ]]; then
        echo "Downloading $bin for $OS-$ARCH"
        curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/${bin}-${OS}-${ARCH} > ${bin}
        chmod +x ${bin}
    else
        echo "$bin already exists"
    fi
done
popd > /dev/null

echo "=== Setup script completed ==="
