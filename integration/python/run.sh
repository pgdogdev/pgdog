#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pushd ${SCRIPT_DIR}/../..
cargo build
popd

pushd ${SCRIPT_DIR}
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

pushd ${SCRIPT_DIR}/../../
target/debug/pgdog --config integration/pgdog.toml --users integration/users.toml > ${SCRIPT_DIR}/pytest_log.txt &
PID=$!
popd

echo "Waiting for PgDog"
while ! pg_isready -h 127.0.0.1 -p 6432 -U pgdog -d pgdog > /dev/null; do
    echo "waiting for PgDog" > /dev/null
done
echo "Running test suite"

pytest

kill -TERM ${PID}
popd
cat ${SCRIPT_DIR}/pytest_log.txt
rm ${SCRIPT_DIR}/pytest_log.txt
