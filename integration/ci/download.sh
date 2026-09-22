#!/usr/bin/env bash
# Download to a file so curl can discard partial data before retrying.
# Piping retries directly into tar can concatenate separate download attempts.
set -euo pipefail

curl --fail --silent --show-error --location \
    --connect-timeout 15 --max-time 120 \
    --retry 3 --retry-all-errors --retry-max-time 300 \
    --output "$2" "$1"
