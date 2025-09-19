#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [[ "$OS" == "darwin" ]]; then
    ARCH=arm64
else
    ARCH=amd64
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
#export PGUSER='pgdog'

for db in pgdog shard_0 shard_1; do
    psql -c "DROP DATABASE $db" || true
    psql -c "CREATE DATABASE $db" || true
    for user in pgdog pgdog1 pgdog2 pgdog3; do
        psql -c "GRANT ALL ON DATABASE $db TO ${user}"
        psql -c "GRANT ALL ON SCHEMA public TO ${user}" ${db}
    done
done

for db in pgdog shard_0 shard_1; do
    for table in sharded sharded_omni; do
            psql -c "DROP TABLE IF EXISTS ${table}" ${db} -U pgdog
            psql -c "CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, value TEXT)" ${db} -U pgdog
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

# Setup GSSAPI test keytabs - always run in CI or when requested
if [ "$CI" = "true" ] || [ -n "$SETUP_GSSAPI" ] || command -v kadmin.local &> /dev/null || [ -x "/opt/homebrew/opt/krb5/sbin/kadmin.local" ]; then
    echo "Setting up GSSAPI test environment..."
    # Make the script executable if it exists
    if [ -f "${SCRIPT_DIR}/gssapi/setup_test_keytabs.sh" ]; then
        chmod +x "${SCRIPT_DIR}/gssapi/setup_test_keytabs.sh"
        bash "${SCRIPT_DIR}/gssapi/setup_test_keytabs.sh" || echo "GSSAPI setup completed (some errors expected)"
    else
        echo "Warning: GSSAPI setup script not found at ${SCRIPT_DIR}/gssapi/setup_test_keytabs.sh"
        # Create the directory structure at least
        mkdir -p "${SCRIPT_DIR}/gssapi/keytabs"
        echo "Created ${SCRIPT_DIR}/gssapi/keytabs directory"
    fi
else
    echo "Skipping GSSAPI setup (not in CI and Kerberos tools not found)"
fi

popd
