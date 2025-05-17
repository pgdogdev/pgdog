#!/bin/bash
set -e

export PGUSER=postgres
export PGPORT=5434
export PGHOST=127.0.0.1
export PGDATABASE=postgres
export PGPASSWORD=postgres

echo "Waiting for Postgres to be ready"
while ! pg_isready; do
    sleep 1
done

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}/../../
cargo run -- --config ${SCRIPT_DIR}/pgdog.toml --users ${SCRIPT_DIR}/users.toml
