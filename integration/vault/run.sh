#!/bin/bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../common.sh
# docker compose resolves docker-compose.yml from the working directory.
cd "${SCRIPT_DIR}"

# Remove once the CI image (docker/Dockerfile.ci) is rebuilt with jq.
if ! command -v jq > /dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq --no-install-recommends jq
fi

setup() {
    VAULT_ADDR="http://127.0.0.1:8200"
    VAULT_TOKEN="root"
    VAULT="docker compose exec -T -e VAULT_TOKEN=$VAULT_TOKEN -e VAULT_ADDR=$VAULT_ADDR vault vault"

    echo "Starting services..."
    docker compose up -d

    # Check over TCP: while running init scripts, the image starts a temporary
    # server that only listens on the Unix socket.
    echo "Waiting for Postgres..."
    until docker compose exec -T postgres pg_isready -h 127.0.0.1 -U postgres &>/dev/null; do sleep 1; done

    echo "Waiting for Vault..."
    until $VAULT status &>/dev/null; do sleep 1; done

    echo "Configuring Vault database secrets engine..."
    $VAULT secrets enable database 2>/dev/null || true

    $VAULT write database/config/pgdog \
        plugin_name=postgresql-database-plugin \
        allowed_roles="pgdog-role,pgdog-static-role" \
        connection_url="postgresql://{{username}}:{{password}}@postgres:5432/pgdog?sslmode=disable" \
        username="postgres" \
        password="postgres"

    $VAULT write database/roles/pgdog-role \
        db_name=pgdog \
        creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT ALL PRIVILEGES ON DATABASE pgdog TO \"{{name}}\"; GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"{{name}}\"; GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"{{name}}\";" \
        revocation_statements="REVOKE ALL PRIVILEGES ON DATABASE pgdog FROM \"{{name}}\"; REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM \"{{name}}\"; REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM \"{{name}}\"; DROP ROLE IF EXISTS \"{{name}}\";" \
        default_ttl="60s" \
        max_ttl="120s"

    $VAULT write database/static-roles/pgdog-static-role \
        db_name=pgdog \
        rotation_statements="ALTER USER \"{{name}}\" WITH PASSWORD '{{password}}';" \
        username="pgdog_static" \
        rotation_period="60s"

    echo "Configuring AppRole auth..."
    $VAULT auth enable approle 2>/dev/null || true

    $VAULT policy write pgdog-policy - <<'EOF'
path "database/creds/pgdog-role" {
    capabilities = ["read"]
}
path "database/static-creds/pgdog-static-role" {
    capabilities = ["read"]
}
EOF

    $VAULT write auth/approle/role/pgdog-role \
        token_policies="pgdog-policy" \
        token_ttl="1m" \
        token_max_ttl="2m"

    ROLE_ID=$($VAULT read -field=role_id auth/approle/role/pgdog-role/role-id)
    SECRET_ID=$($VAULT write -f -field=secret_id auth/approle/role/pgdog-role/secret-id)

    echo "Writing pgdog config..."
    echo "$SECRET_ID" > "$SCRIPT_DIR/vault-secret-id"
    cat > "$SCRIPT_DIR/pgdog.toml" <<EOF
[general]
host = "0.0.0.0"
port = 6432
log_level = "warn"
# Recycle server connections constantly so new backend logins race Vault's
# static role rotation.
server_lifetime = 2000

[admin]
name = "admin"
password = "pgdog"
user = "admin"

[[databases]]
name = "pgdog"
host = "127.0.0.1"
port = 5450
role = "primary"

[vault]
url = "http://127.0.0.1:8200"
auth_method = "approle"
approle_role_id = "$ROLE_ID"
approle_secret_id_file = "$SCRIPT_DIR/vault-secret-id"
EOF
}

# Safety net: pgdog and the containers are cleaned up on exit even if dev.sh
# is killed mid-flight by timeout or signal.
cleanup() {
    if [ -f "${COMMON_DIR}/pgdog.pid" ]; then
        stop_pgdog
    fi
    docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

setup

# Stay within the 10 minute CI job timeout.
timeout --signal=TERM --kill-after=30s 7m bash "${SCRIPT_DIR}/dev.sh"
