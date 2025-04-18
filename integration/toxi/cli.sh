#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CLI="$SCRIPT_DIR/../toxiproxy-cli"
PROXY=primary

if [[ "$1"  == "timeout" ]]; then
    ${CLI} toxic add --toxicName timeout --type timeout ${PROXY}
elif [[ "$1" == "clear" ]]; then
    ${CLI} toxic remove --toxicName timeout ${PROXY} || true
    ${CLI} toxic remove --toxicName reset_peer ${PROXY} || true
elif [[ "$1" == "reset" ]]; then
    ${CLI} toxic add --toxicName reset_peer --type reset_peer ${PROXY}
else
    ${CLI} $@
fi
