#!/usr/bin/env bash
# Print the source hash used to key the pgdog binary cache. Hashes every
# file whose change should invalidate the built `pgdog` binary, and nothing
# else (no `target/`, no examples/tests, no unrelated subcrates).
#
# Keep the file/dir lists in sync with the workspace crates that link into
# `pgdog`.
set -euo pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/../.." && pwd )"

cd "$REPO_ROOT"

files=(
    Cargo.lock
    Cargo.toml
    .cargo/config.toml
    pgdog/Cargo.toml
    pgdog/build.rs
    pgdog-config/Cargo.toml
    pgdog-macros/Cargo.toml
    pgdog-plugin/Cargo.toml
    pgdog-plugin/build.rs
    pgdog-postgres-types/Cargo.toml
    pgdog-stats/Cargo.toml
    pgdog-vector/Cargo.toml
)

dirs=(
    pgdog/src
    pgdog-config/src
    pgdog-macros/src
    pgdog-plugin/src
    pgdog-postgres-types/src
    pgdog-stats/src
    pgdog-vector/src
)

{
    for f in "${files[@]}"; do
        cksum "$f"
    done
    for d in "${dirs[@]}"; do
        find "$d" -type f -name '*.rs' -print0 | LC_ALL=C sort -z | xargs -0 cksum
    done
} | cksum | cut -d' ' -f1
