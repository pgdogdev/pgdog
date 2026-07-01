#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

VAULT_ADDR="http://127.0.0.1:8200"
VAULT_TOKEN="root"
VAULT="docker compose exec -T -e VAULT_TOKEN=$VAULT_TOKEN -e VAULT_ADDR=$VAULT_ADDR vault vault"

echo "Starting services..."
docker compose up -d

echo "Waiting for Postgres..."
until docker compose exec -T postgres pg_isready -U postgres &>/dev/null; do sleep 1; done

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
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT ALL PRIVILEGES ON DATABASE pgdog TO \"{{name}}\";" \
    revocation_statements="REVOKE ALL PRIVILEGES ON DATABASE pgdog FROM \"{{name}}\"; DROP ROLE IF EXISTS \"{{name}}\";" \
    default_ttl="10m" \
    max_ttl="30m"

echo "Creating static Postgres user for Vault static role..."
docker compose exec -T postgres psql -U postgres -c \
    "CREATE USER pgdog_static WITH LOGIN PASSWORD 'initial_password'; \
     GRANT ALL PRIVILEGES ON DATABASE pgdog TO pgdog_static;" \
    2>/dev/null || true

echo "Configuring Vault static database role..."
$VAULT write database/static-roles/pgdog-static-role \
    db_name=pgdog \
    rotation_statements="ALTER USER \"{{name}}\" WITH PASSWORD '{{password}}';" \
    username="pgdog_static" \
    rotation_period=300

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
    token_ttl=1h \
    token_max_ttl=4h

ROLE_ID=$($VAULT read -field=role_id auth/approle/role/pgdog-role/role-id)
SECRET_ID=$($VAULT write -f -field=secret_id auth/approle/role/pgdog-role/secret-id)
STATIC_PWD=$($VAULT read -field=password database/static-creds/pgdog-static-role)

echo "$SECRET_ID" > "$SCRIPT_DIR/vault-secret-id"
echo "Written vault-secret-id"

cat > "$SCRIPT_DIR/pgdog.toml" <<EOF
[general]
host = "0.0.0.0"
port = 6432

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

echo "Generated pgdog.toml with role_id=$ROLE_ID"
echo ""
echo "Static role password is $STATIC_PWD"
echo ""
echo "Run pgdog with:"
echo "  cargo run -- --config $SCRIPT_DIR/pgdog.toml --users $SCRIPT_DIR/users.toml"
