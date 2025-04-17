#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CLI="$SCRIPT_DIR/../toxiproxy-cli"

if [[ "$1"  == "timeout" ]]; then
    ${CLI} toxic add --toxicName timeout --type timeout postgres
elif [[ "$1" == "clear" ]]; then
    ${CLI} toxic remove --toxicName timeout postgres
else
    ${CLI} $@
fi
