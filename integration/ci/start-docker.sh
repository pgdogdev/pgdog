#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f /.dockerenv ]]; then
    docker info >/dev/null
    exit 0
fi

SOCKET=/var/run/docker-nested.sock
mirrors=()
while read -r mirror; do
    [[ -n "$mirror" ]] && mirrors+=(--registry-mirror "$mirror")
done < <(docker info --format '{{range .RegistryConfig.Mirrors}}{{println .}}{{end}}' 2>/dev/null || true)

sudo sh -c "nohup dockerd --host unix://${SOCKET} --pidfile /var/run/docker-nested.pid ${mirrors[*]} > /var/log/dockerd.log 2>&1 &"

export DOCKER_HOST="unix://${SOCKET}"
if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "DOCKER_HOST=${DOCKER_HOST}" >> "$GITHUB_ENV"
fi

for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
        exit 0
    fi
    sleep 0.5
done

echo "dockerd did not become ready" >&2
sudo cat /var/log/dockerd.log >&2
exit 1
