#!/usr/bin/env bash
set -euo pipefail

COUNT=2000
HOST="127.0.0.1"
PORT=15432

while [[ $# -gt 0 ]]; do
    case "$1" in
        --count) COUNT="$2"; shift 2 ;;
        --host)  HOST="$2";  shift 2 ;;
        --port)  PORT="$2";  shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

PGCONN="postgresql://postgres:postgres@${HOST}:${PORT}/postgres"

created=0
skipped=0
start_time=$(date +%s)

echo "Creating ${COUNT} tenant databases on ${HOST}:${PORT}..."
echo "Using tenant_template as base (schema + seed data pre-applied)"

# Verify template exists before bulk creation
if ! psql "$PGCONN" -tAc "SELECT 1 FROM pg_database WHERE datname = 'tenant_template'" | grep -q 1; then
    echo "ERROR: tenant_template database does not exist. Run init.sql first."
    exit 1
fi

for i in $(seq 1 "$COUNT"); do
    dbname="tenant_$i"
    exists=$(psql "$PGCONN" -tAc "SELECT 1 FROM pg_database WHERE datname = '${dbname}'" 2>/dev/null || true)

    if [[ "$exists" == "1" ]]; then
        ((skipped++)) || true
    else
        # Disconnect any sessions from template before cloning
        psql "$PGCONN" -qc "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'tenant_template' AND pid <> pg_backend_pid();" 2>/dev/null || true
        psql "$PGCONN" -qc "CREATE DATABASE ${dbname} TEMPLATE tenant_template OWNER pgdog;" 2>/dev/null
        ((created++)) || true
    fi

    if (( i % 100 == 0 )); then
        elapsed=$(( $(date +%s) - start_time ))
        echo "  [${i}/${COUNT}] created=${created} skipped=${skipped} elapsed=${elapsed}s"
    fi
done

echo ""
echo "Creating test users for passthrough auth testing..."

user_sql=""
for i in $(seq 1 100); do
    user_sql+="DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'tenant_user_${i}') THEN
    CREATE ROLE tenant_user_${i} WITH LOGIN PASSWORD 'pass_${i}';
  END IF;
END \$\$;
"
done
psql "$PGCONN" -qc "$user_sql"

# Grant connect on all tenant databases to pgdog and test users
echo "Granting access to tenant databases..."
grant_sql=""
for i in $(seq 1 "$COUNT"); do
    grant_sql+="GRANT CONNECT ON DATABASE tenant_${i} TO pgdog;"
done
for i in $(seq 1 100); do
    grant_sql+="GRANT CONNECT ON DATABASE tenant_1 TO tenant_user_${i};"
done
psql "$PGCONN" -qc "$grant_sql"

# Grant table-level access inside each tenant DB for test users
# (pgdog is superuser so needs no explicit grants)
for i in $(seq 1 100); do
    psql "postgresql://postgres:postgres@${HOST}:${PORT}/tenant_1" \
        -qc "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO tenant_user_${i};
             GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO tenant_user_${i};" 2>/dev/null || true
done

elapsed=$(( $(date +%s) - start_time ))
echo ""
echo "=== Summary ==="
echo "  Total requested: ${COUNT}"
echo "  Created:         ${created}"
echo "  Skipped:         ${skipped}"
echo "  Test users:      100 (tenant_user_1..tenant_user_100)"
echo "  Elapsed:         ${elapsed}s"
