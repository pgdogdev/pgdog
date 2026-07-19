#!/usr/bin/env bash
# Configure toxiproxy to proxy traffic to Postgres primary.
# Run after docker-compose up and before PgDog starts.
set -euo pipefail

TOXI_API="${TOXI_API:-http://127.0.0.1:8474}"
PG_HOST="${PG_HOST:-postgres_primary}"
PG_PORT="${PG_PORT:-5432}"
LISTEN_PORT="${LISTEN_PORT:-5432}"

# Wait for toxiproxy API
for i in $(seq 1 15); do
    if curl -sf "${TOXI_API}/version" >/dev/null 2>&1; then
        echo "Toxiproxy API ready"
        break
    fi
    sleep 1
    if [ "$i" -eq 15 ]; then
        echo "ERROR: Toxiproxy API not reachable at ${TOXI_API}"
        exit 1
    fi
done

# Create proxy: PgDog connects to toxiproxy:15440, which forwards to postgres:5432
RESPONSE=$(curl -sf -X POST "${TOXI_API}/proxies" \
    -H "Content-Type: application/json" \
    -d "{
        \"name\": \"pg_primary\",
        \"listen\": \"0.0.0.0:${LISTEN_PORT}\",
        \"upstream\": \"${PG_HOST}:${PG_PORT}\",
        \"enabled\": true
    }" 2>&1) || {
    echo "WARNING: Could not create proxy (may already exist): ${RESPONSE}"
    # If it already exists, that's fine
    curl -sf "${TOXI_API}/proxies/pg_primary" >/dev/null 2>&1 || {
        echo "ERROR: Proxy does not exist and could not be created"
        exit 1
    }
}

echo "Proxy created: 0.0.0.0:${LISTEN_PORT} -> ${PG_HOST}:${PG_PORT}"
