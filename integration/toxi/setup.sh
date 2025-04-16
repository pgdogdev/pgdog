#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CLI="$SCRIPT_DIR/../toxiproxy-cli"

${CLI} delete postgres || true
${CLI} create --listen :5435 --upstream :5432 postgres
${CLI} toxic add -t latency -n toxicLatency -t latency -a latency=200 postgres
${CLI} inspect postgres
