#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd ${SCRIPT_DIR}
bash go_pgx/run.sh
bash go_pq/run.sh
bash go_gorm/run.sh
popd
