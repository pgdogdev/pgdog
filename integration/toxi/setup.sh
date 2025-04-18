#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CLI="$SCRIPT_DIR/../toxiproxy-cli"

${CLI} delete primary || true
${CLI} delete replica || true
${CLI} create --listen :5435 --upstream :5432 primary
${CLI} create --listen :5436 --upstream :5432 replica
${CLI} toxic add -t latency -n toxicLatency -t latency -a latency=30 primary
${CLI} toxic add -t latency -n toxicLatency -t latency -a latency=30 replica
${CLI} inspect primary
${CLI} inspect replica
