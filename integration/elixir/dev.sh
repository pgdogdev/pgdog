#!/bin/bash
#
# Fetch deps and run the ExUnit suite against an already-running PgDog
# (see integration/dev-server.sh).
#
# Elixir/Erlang have to already be on PATH: CI installs them with
# erlef/setup-beam, local dev uses whatever the developer has (asdf, mise,
# Homebrew, ...).
#
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if ! command -v mix > /dev/null; then
    echo "mix not found. Install Elixir (https://elixir-lang.org/install.html) and re-run." >&2
    exit 1
fi

pushd ${SCRIPT_DIR}

export MIX_ENV=test

mix local.hex --force --if-missing
mix local.rebar --force --if-missing
mix deps.get
mix test

popd
