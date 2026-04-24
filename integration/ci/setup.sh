#!/usr/bin/env bash
# Bootstrap PostgreSQL + pgdog's test fixtures on a Blacksmith runner.
# Called from workflow jobs after install-deps.sh.
#
# Usage: integration/ci/setup.sh [--with-toxi]
set -euo pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/../.." && pwd )"

# Use whatever pg cluster is installed on the runner unless overridden.
if [[ -z "${PSQL_VERSION:-}" ]]; then
    PSQL_VERSION=$(ls -d /etc/postgresql/*/main 2>/dev/null \
        | sed 's|.*/postgresql/\([0-9]\+\)/main|\1|' \
        | sort -n | tail -1 || true)
fi
if [[ -z "${PSQL_VERSION:-}" ]]; then
    echo "No PostgreSQL cluster found under /etc/postgresql" >&2
    exit 1
fi

with_toxi=false
for arg in "$@"; do
    case "$arg" in
        --with-toxi) with_toxi=true ;;
        *) echo "unknown arg: $arg" >&2; exit 1 ;;
    esac
done

sudo pg_ctlcluster "$PSQL_VERSION" main start

USER_NAME="$(id -un)"
sudo -u postgres createuser --superuser --login "$USER_NAME"
sudo -u postgres createdb "$USER_NAME"

bash "${REPO_ROOT}/integration/setup.sh" \
    || (sudo pg_ctlcluster "$PSQL_VERSION" main restart && bash "${REPO_ROOT}/integration/setup.sh")

if [[ "$with_toxi" == true ]]; then
    bash "${REPO_ROOT}/integration/toxi/setup.sh"
fi
