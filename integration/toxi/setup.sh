#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CLI="$SCRIPT_DIR/../toxiproxy-cli"

killall toxiproxy-server || true
${SCRIPT_DIR}/../toxiproxy-server > /dev/null &
sleep 1

${CLI} delete primary || true
${CLI} delete replica || true
${CLI} create --listen :5435 --upstream :5432 primary
${CLI} create --listen :5436 --upstream :5432 replica
${CLI} create --listen :5437 --upstream :5432 replica2
${CLI} create --listen :5438 --upstream :5432 replica3
${CLI} list
