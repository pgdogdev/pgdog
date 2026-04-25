#!/usr/bin/env bash
# Build an llvm-cov-instrumented pgdog binary for the integration job and
# export its path via $GITHUB_ENV so later steps can invoke it.
set -euo pipefail

export RUSTFLAGS="-C link-dead-code"

cargo llvm-cov clean --workspace
mkdir -p target/llvm-cov-target/profiles
cargo llvm-cov run --no-report --release --package pgdog --bin pgdog -- --help
rm -f target/llvm-cov-target/profiles/*.profraw
rm -f target/llvm-cov-target/profiles/.last_snapshot
rm -rf target/llvm-cov-target/reports

BIN_PATH=$(find target/llvm-cov-target -type f -path '*/release/pgdog' | head -n 1)
if [ -z "$BIN_PATH" ]; then
    echo "Instrumented PgDog binary not found" >&2
    exit 1
fi
echo "Using instrumented binary at $BIN_PATH"

RESOLVED="$(realpath "$BIN_PATH")"
if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "PGDOG_BIN=${RESOLVED}" >> "$GITHUB_ENV"
else
    echo "PGDOG_BIN=${RESOLVED}"
fi
