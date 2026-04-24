#!/usr/bin/env bash
# Bootstrap PostgreSQL + pgdog's test fixtures inside the pgdog-ci-base
# container. Called from .github/workflows/ci.yml.
#
# Usage: integration/ci/setup.sh [--with-toxi]
set -euo pipefail

PSQL_VERSION="${PSQL_VERSION:-18}"
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/../.." && pwd )"

with_toxi=false
for arg in "$@"; do
    case "$arg" in
        --with-toxi) with_toxi=true ;;
        *) echo "unknown arg: $arg" >&2; exit 1 ;;
    esac
done

pg_ctlcluster "$PSQL_VERSION" main start

USER_NAME="$(id -un)"
sudo -u postgres createuser --superuser --login "$USER_NAME"
sudo -u postgres createdb "$USER_NAME"

bash "${REPO_ROOT}/integration/setup.sh" \
    || (pg_ctlcluster "$PSQL_VERSION" main restart && bash "${REPO_ROOT}/integration/setup.sh")

if [[ "$with_toxi" == true ]]; then
    bash "${REPO_ROOT}/integration/toxi/setup.sh"
fi
