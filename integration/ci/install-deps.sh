#!/usr/bin/env bash
# Install the bits that the Blacksmith runner image doesn't already ship.
# Idempotent — safe to run once per job. Fast when everything is already
# present (most of the time).
set -euo pipefail

NEXTEST_VERSION="${NEXTEST_VERSION:-0.9.78}"
LLVM_COV_VERSION="${LLVM_COV_VERSION:-0.6.10}"
CMAKE_VERSION="${CMAKE_VERSION:-3.31.6}"

need_apt=()
for pkg in mold php-pgsql python3-virtualenv; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        need_apt+=("$pkg")
    fi
done
if (( ${#need_apt[@]} )); then
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends "${need_apt[@]}"
fi

# Kitware prebuilt cmake pinned to the version pgdog expects. Much faster
# than `pip install cmake` which compiles from source.
if ! cmake --version 2>/dev/null | head -1 | grep -q "$CMAKE_VERSION"; then
    curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" \
        | sudo tar xzf - -C /opt
    sudo ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/cmake" /usr/local/bin/cmake
    sudo ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/ctest" /usr/local/bin/ctest
fi

CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$CARGO_BIN"

if ! command -v cargo-nextest >/dev/null; then
    curl -LsSf "https://get.nexte.st/${NEXTEST_VERSION}/linux" | tar zxf - -C "$CARGO_BIN"
fi

if ! command -v cargo-llvm-cov >/dev/null; then
    curl -LsSf "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${LLVM_COV_VERSION}/cargo-llvm-cov-x86_64-unknown-linux-gnu.tar.gz" \
        | tar zxf - -C "$CARGO_BIN"
fi

if ! command -v bundle >/dev/null; then
    sudo gem install bundler --no-document
fi
