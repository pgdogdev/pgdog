#!/bin/bash
#
# Run the suite against an already-running PgDog (see integration/dev-server.sh).
#
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/common.sh"

dev_suite
