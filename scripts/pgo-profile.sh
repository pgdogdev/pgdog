#!/bin/bash
#
# Profile-guided optimization (PGO) compiles PgDog with a profile of how often
# each branch within our code runs under realistic traffic.
#
# The compiler optimizes frequently run code for speed. HOWEVER: Code the training
# workload doesn't cover can be slightly slower than in a regular build.
# This is why this uses our integration suites as opposed to custom scripts.
#
# On x86, PGO builds handle roughly 10% more transactions per second while using
# about 10% less CPU per transaction. Code outside the hot path, e.g., COPY
# and connection setup, were benchmarked in a test and found to be
# unchanged on x86 and actually slightly faster on Graviton (than a bare release build).
#
# TODO(jkaczman): the integration suites weight code by how **often** tests hit them, not by
# production traffic. A replay of real traffic or a TPC-C run would be more representative.
#
# TODO(jkaczman): COPY is measured, but still need to test some things to ensure no regressions,
# e.g., schema sync, data sync and logical replication.
#
# TODO(jkaczman): compare PGO and regular release builds via Bencher to check for regressions & speedup

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
RAW_PROFILES_DIR=$REPO_ROOT/target/pgo-raw
INSTRUMENTED_TARGET_DIR=$REPO_ROOT/target/pgo-instrumented
TARGET_TRIPLE=${1:-}

TRAINING_SUITES=(sql python pgbench complex schema_sync tls two_pc copy_data/data_sync)

build_instrumented_pgdog() {
    local target_args=()
    if [ -n "$TARGET_TRIPLE" ]; then
        target_args=(--target "$TARGET_TRIPLE")
    fi

    cargo build --release -p pgdog "${target_args[@]}" \
        --target-dir "$INSTRUMENTED_TARGET_DIR" \
        --config "target.'cfg(all())'.rustflags=['-Cprofile-generate=$RAW_PROFILES_DIR']"
}

record_training_profiles() {
    export PGDOG_BIN=$INSTRUMENTED_TARGET_DIR/$TARGET_TRIPLE/release/pgdog

    local suite
    for suite in "${TRAINING_SUITES[@]}"; do
        bash "integration/$suite/run.sh"
    done
}

merge_profiles() {
    local llvm_profdata
    llvm_profdata=$(find "$(rustc --print sysroot)" -name llvm-profdata -type f | head -1)

    "$llvm_profdata" merge -o pgo.profdata "$RAW_PROFILES_DIR"/*.profraw
    echo "Profile written to $REPO_ROOT/pgo.profdata"
}

cd "$REPO_ROOT"
rm -rf "$RAW_PROFILES_DIR"

build_instrumented_pgdog
record_training_profiles
merge_profiles
