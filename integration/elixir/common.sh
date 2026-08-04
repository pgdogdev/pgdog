#!/bin/bash
#
# Shared helpers for the Elixir integration suite.
# Source this file; do not execute directly.
#
ELIXIR_COMMON_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${ELIXIR_COMMON_DIR}/../common.sh"

# Fetch deps and run the ExUnit suite.
#
# Elixir/Erlang have to already be on PATH: CI installs them with
# erlef/setup-beam, local dev uses whatever the developer has (asdf, mise,
# Homebrew, ...).
function dev_suite() {
    if ! command -v mix > /dev/null; then
        echo "mix not found. Install Elixir (https://elixir-lang.org/install.html) and re-run." >&2
        exit 1
    fi

    pushd "${ELIXIR_COMMON_DIR}"

    export MIX_ENV=test

    mix local.hex --force --if-missing
    mix local.rebar --force --if-missing
    mix deps.get
    mix test

    popd
}
