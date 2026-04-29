#!/usr/bin/env bash
set -euo pipefail

## Configure Vault's database secrets engine for the pgdog vault integration.
## Run after docker compose up.

export VAULT_ADDR="http://127.0.0.1:18200"
export VAULT_TOKEN="root-token"

echo "==> Enabling database secrets engine..."
vault secrets enable database 2>/dev/null || echo "    (already enabled)"

echo "==> Configuring PostgreSQL connection..."
vault write database/config/pgdog \
    plugin_name=postgresql-database-plugin \
    allowed_roles="pgdog-admin,dml-role,ddl-role,readonly-role" \
    connection_url="postgresql://{{username}}:{{password}}@host.docker.internal:15432/pgdog?sslmode=disable" \
    username="postgres" \
    password="postgres"

echo "==> Creating DML role (30m TTL)..."
vault write database/roles/dml-role \
    db_name=pgdog \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}' IN ROLE \"dml_role\";" \
    revocation_statements="DROP ROLE IF EXISTS \"{{name}}\";" \
    default_ttl="30m" \
    max_ttl="1h"

echo "==> Creating DDL role (30m TTL)..."
vault write database/roles/ddl-role \
    db_name=pgdog \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}' IN ROLE \"ddl_role\";" \
    revocation_statements="DROP ROLE IF EXISTS \"{{name}}\";" \
    default_ttl="30m" \
    max_ttl="1h"

echo "==> Creating readonly role (30m TTL)..."
vault write database/roles/readonly-role \
    db_name=pgdog \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}' IN ROLE \"readonly_role\";" \
    revocation_statements="DROP ROLE IF EXISTS \"{{name}}\";" \
    default_ttl="30m" \
    max_ttl="1h"

echo "==> Creating pgdog-admin role (for pg_authid access, 30m TTL)..."
vault write database/roles/pgdog-admin \
    db_name=pgdog \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}' IN ROLE \"pgdog_admin_role\"; GRANT pg_read_all_data TO \"{{name}}\";" \
    revocation_statements="DROP ROLE IF EXISTS \"{{name}}\";" \
    default_ttl="30m" \
    max_ttl="1h"

echo "==> Enabling AppRole auth..."
vault auth enable approle 2>/dev/null || echo "    (already enabled)"

echo "==> Creating pgdog AppRole..."
vault write auth/approle/role/pgdog \
    token_ttl=1h \
    token_max_ttl=4h \
    token_policies="pgdog-policy"

echo "==> Creating pgdog policy..."
vault policy write pgdog-policy - <<'POLICY'
path "database/creds/*" {
  capabilities = ["read"]
}

# Renew leases
path "sys/leases/renew" {
  capabilities = ["update"]
}
POLICY

echo "==> Fetching AppRole credentials..."
ROLE_ID=$(vault read -field=role_id auth/approle/role/pgdog/role-id)
SECRET_ID=$(vault write -field=secret_id -f auth/approle/role/pgdog/secret-id)

echo ""
echo "============================================"
echo "  Vault setup complete!"
echo "============================================"
echo ""
echo "  AppRole role_id:  $ROLE_ID"
echo "  AppRole secret_id: $SECRET_ID"
echo ""
echo "  Test credential generation:"
echo "    vault read database/creds/dml-role"
echo "    vault read database/creds/ddl-role"
echo "    vault read database/creds/readonly-role"
echo ""
echo "  To use with pgdog, update integration/vault/pgdog.toml with:"
echo "    role_id = \"$ROLE_ID\""
echo "    secret_id = \"$SECRET_ID\""
echo ""
