#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh

run_pgdog integration/tls

# pg_isready doesn't present a client cert, so use run_psql from dev.sh instead.
source ${SCRIPT_DIR}/dev.sh --source-only

echo "Waiting for PgDog"
while ! run_psql pgdog client; do
    sleep 0.1
done
echo "PgDog is ready"

bash ${SCRIPT_DIR}/dev.sh

stop_pgdog
