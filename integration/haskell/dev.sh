#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd "${SCRIPT_DIR}"

has_required_version() {
    cabal list persistent-postgresql --simple-output |
        grep -Fqx 'persistent-postgresql 2.14.3.0'
}

for attempt in 1 2 3; do
    if has_required_version; then
        break
    fi

    echo "Hackage index is stale; refreshing (attempt ${attempt}/3)" >&2
    cabal update || true
    sleep $((attempt * 5))
done

if ! has_required_version; then
    echo "Hackage index does not contain persistent-postgresql 2.14.3.0 after 3 refresh attempts" >&2
    exit 1
fi

cabal test --test-show-details=direct
popd
