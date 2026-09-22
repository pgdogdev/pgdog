#!/usr/bin/env bash
# Install the bits that the Blacksmith runner image doesn't already ship.
# Linux/Debian-derivative only — no-op on other platforms. Idempotent.
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
    exit 0
fi
if ! command -v apt-get >/dev/null 2>&1 || ! command -v dpkg >/dev/null 2>&1; then
    echo "install-deps.sh requires a Debian/Ubuntu runner (apt-get + dpkg)" >&2
    exit 1
fi

NEXTEST_VERSION="${NEXTEST_VERSION:-0.9.78}"
LLVM_COV_VERSION="${LLVM_COV_VERSION:-0.6.10}"
CMAKE_VERSION="${CMAKE_VERSION:-3.31.6}"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
downloads=$(mktemp -d)
trap 'rm -rf -- "$downloads"' EXIT

if ! dpkg -s mold gdb >/dev/null 2>&1; then
    sudo apt-get -o Acquire::Retries=3 -o APT::Update::Error-Mode=any update
    sudo apt-get -o Acquire::Retries=3 install -y --no-install-recommends mold gdb
fi

# The protocol_version smoke test needs a psql >= 18 client. The runner
# image ships psql 16, so pull the v18 client from PGDG. pg_wrapper picks
# the highest installed major, so /usr/bin/psql will resolve to 18.
if ! dpkg -s postgresql-client-18 >/dev/null 2>&1; then
    sudo install -d /usr/share/postgresql-common/pgdg
    bash "$SCRIPT_DIR/download.sh" https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        "$downloads/pgdg.asc"
    sudo install -m 644 "$downloads/pgdg.asc" /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
    . /etc/os-release
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
        | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
    sudo apt-get -o Acquire::Retries=3 -o APT::Update::Error-Mode=any update
    sudo apt-get -o Acquire::Retries=3 install -y --no-install-recommends postgresql-client-18
fi

# Kitware prebuilt cmake pinned to the version pgdog expects. Much faster
# than `pip install cmake` which compiles from source.
if ! cmake --version 2>/dev/null | head -1 | grep -q "$CMAKE_VERSION"; then
    bash "$SCRIPT_DIR/download.sh" \
        "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" \
        "$downloads/cmake.tar.gz"
    sudo tar xzf "$downloads/cmake.tar.gz" -C /opt
    sudo ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/cmake" /usr/local/bin/cmake
    sudo ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/ctest" /usr/local/bin/ctest
fi

CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$CARGO_BIN"

if ! command -v cargo-nextest >/dev/null; then
    bash "$SCRIPT_DIR/download.sh" "https://get.nexte.st/${NEXTEST_VERSION}/linux" "$downloads/nextest.tar.gz"
    tar zxf "$downloads/nextest.tar.gz" -C "$CARGO_BIN"
fi

if ! command -v cargo-llvm-cov >/dev/null; then
    bash "$SCRIPT_DIR/download.sh" \
        "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${LLVM_COV_VERSION}/cargo-llvm-cov-x86_64-unknown-linux-gnu.tar.gz" \
        "$downloads/llvm-cov.tar.gz"
    tar zxf "$downloads/llvm-cov.tar.gz" -C "$CARGO_BIN"
fi

# cargo llvm-cov report needs llvm-profdata/llvm-cov from llvm-tools-preview.
if command -v rustup >/dev/null 2>&1; then
    if ! rustup component list --installed 2>/dev/null | grep -q llvm-tools; then
        rustup component add llvm-tools-preview || true
    fi
fi
