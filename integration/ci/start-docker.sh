#!/usr/bin/env bash
set -euo pipefail

if docker info >/dev/null 2>&1; then
    exit 0
fi

sudo sh -c 'nohup dockerd > /var/log/dockerd.log 2>&1 &'

for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
        exit 0
    fi
    sleep 0.5
done

echo "dockerd did not become ready" >&2
sudo cat /var/log/dockerd.log >&2
exit 1
