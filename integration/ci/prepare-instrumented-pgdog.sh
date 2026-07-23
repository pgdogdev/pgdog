#!/usr/bin/env bash
# Build an llvm-cov-instrumented pgdog binary for the integration job and
# export its path via $GITHUB_ENV so later steps can invoke it.
#
# Usage: prepare-instrumented-pgdog.sh [debug|release]  (default: release)
set -euo pipefail

PROFILE="${1:-release}"
if [[ "${PROFILE}" != "debug" && "${PROFILE}" != "release" ]]; then
    echo "Usage: $0 [debug|release]" >&2
    exit 1
fi

PROFILE_FLAG=""
if [[ "${PROFILE}" == "release" ]]; then
    PROFILE_FLAG="--release"
fi

# RUSTFLAGS overrides .cargo/config.toml rustflags entirely, so
# tokio_unstable (set there) must be repeated here.
export RUSTFLAGS="--cfg tokio_unstable -C link-dead-code"

cargo llvm-cov clean --workspace
mkdir -p target/llvm-cov-target/profiles
# shellcheck disable=SC2086
cargo llvm-cov run --no-report ${PROFILE_FLAG} --package pgdog --bin pgdog -- --help
rm -f target/llvm-cov-target/profiles/*.profraw
rm -f target/llvm-cov-target/profiles/.last_snapshot
rm -rf target/llvm-cov-target/reports

BIN_PATH=$(find target/llvm-cov-target -type f -path "*/${PROFILE}/pgdog" | head -n 1)
if [ -z "$BIN_PATH" ]; then
    echo "Instrumented PgDog binary (${PROFILE}) not found" >&2
    exit 1
fi
echo "Using instrumented binary at $BIN_PATH"

RESOLVED="$(realpath "$BIN_PATH")"
if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "PGDOG_BIN=${RESOLVED}" >> "$GITHUB_ENV"
else
    echo "PGDOG_BIN=${RESOLVED}"
fi
