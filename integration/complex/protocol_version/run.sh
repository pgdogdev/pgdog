#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../common.sh

PSQL_BIN="${PSQL_BIN:-$(command -v psql || true)}"
if [[ -z "${PSQL_BIN}" ]]; then
    echo "psql not found; PostgreSQL 18+ client is required for protocol_version integration test"
    exit 1
fi

PSQL_VERSION="$(${PSQL_BIN} --version)"
if [[ ! "${PSQL_VERSION}" =~ [[:space:]](1[89]|[2-9][0-9])\. ]]; then
    echo "PostgreSQL 18+ client is required for protocol_version integration test, found: ${PSQL_VERSION}"
    exit 1
fi

PGDOG_PORT="${PGDOG_PORT:-7432}"
OPENMETRICS_PORT="${OPENMETRICS_PORT:-19090}"
HEALTHCHECK_PORT="${HEALTHCHECK_PORT:-18080}"
CONFIG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pgdog-protocol-version.XXXXXX")"

# Run this smoke test on a throwaway listener port so it can coexist with a
# developer's normal PgDog / Docker setup.
awk \
    -v port="${PGDOG_PORT}" \
    -v openmetrics_port="${OPENMETRICS_PORT}" \
    -v healthcheck_port="${HEALTHCHECK_PORT}" \
    '
    /^\[general\]$/ { print; print "port = " port; next }
    /^openmetrics_port = / { print "openmetrics_port = " openmetrics_port; next }
    /^healthcheck_port = / { print "healthcheck_port = " healthcheck_port; next }
    { print }
    ' \
    "${SCRIPT_DIR}/../../pgdog.toml" > "${CONFIG_DIR}/pgdog.toml"
cp "${SCRIPT_DIR}/../../users.toml" "${CONFIG_DIR}/users.toml"

cleanup() {
    stop_pgdog >/dev/null 2>&1 || true
    rm -rf "${CONFIG_DIR}"
}
trap cleanup EXIT

run_pgdog "${CONFIG_DIR}"

until pg_isready -h 127.0.0.1 -p "${PGDOG_PORT}" -U pgdog -d pgdog > /dev/null; do
    sleep 1
done

run_psql_with_protocol() {
    local protocol_version="$1"
    local result

    # This is the independent client check that complements the raw-wire Rust
    # tests: libpq rejects the connection unless the requested protocol version
    # can actually be negotiated.
    result="$("${PSQL_BIN}" -X -tA "postgresql://pgdog:pgdog@127.0.0.1:${PGDOG_PORT}/pgdog?sslmode=disable&gssencmode=disable&min_protocol_version=${protocol_version}&max_protocol_version=${protocol_version}" -c 'SELECT 1')"
    if [[ "${result}" != "1" ]]; then
        echo "expected successful libpq ${protocol_version} query, got: ${result}"
        exit 1
    fi
}

run_psql_with_protocol "3.0"
run_psql_with_protocol "3.2"

echo "PASS: psql connected with min=max protocol versions 3.0 and 3.2"
