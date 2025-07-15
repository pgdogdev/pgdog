#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [[ "$OS" == "darwin" ]]; then
    ARCH=arm64
else
    ARCH=amd64
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track what we've created for rollback
CREATED_USERS=()
CREATED_DATABASES=()

# Cleanup function for errors
cleanup_on_error() {
    echo -e "${RED}Setup failed! Rolling back changes...${NC}"
    
    # Drop created databases
    for db in "${CREATED_DATABASES[@]}"; do
        echo "Dropping database $db..."
        psql -c "DROP DATABASE IF EXISTS $db" 2>/dev/null || true
    done
    
    # Drop created users
    for user in "${CREATED_USERS[@]}"; do
        echo "Dropping user $user..."
        psql -c "DROP USER IF EXISTS $user" 2>/dev/null || true
    done
    
    echo -e "${RED}Rollback complete. Please fix the issues and try again.${NC}"
    exit 1
}

# Set trap for cleanup on error
trap cleanup_on_error ERR

echo "=== PgDog Integration Test Setup ==="
echo

# Run prerequisite checks first
echo "Running prerequisite checks..."
if ! "${SCRIPT_DIR}/check-prerequisites.sh"; then
    echo -e "${RED}Prerequisites not met. Please fix the issues above.${NC}"
    exit 1
fi

echo
echo "Starting setup..."

# Step 1: Create users
echo -e "${GREEN}[1/5]${NC} Creating test users..."
for user in pgdog pgdog1 pgdog2 pgdog3; do
    if psql -c "CREATE USER ${user} LOGIN SUPERUSER PASSWORD 'pgdog'" 2>/dev/null; then
        echo "  ✓ Created user: $user"
        CREATED_USERS+=("$user")
    else
        echo "  ⚠ User $user already exists (skipping)"
    fi
done

# GitHub Actions fix
if [[ "$USER" == "runner" ]]; then
    echo "  ✓ Configured GitHub Actions runner user"
    psql -c "ALTER USER runner PASSWORD 'pgdog' LOGIN;" || true
fi

# Set PostgreSQL environment
export PGPASSWORD='pgdog'
export PGHOST=127.0.0.1
export PGPORT=5432

# Step 2: Create databases
echo -e "${GREEN}[2/5]${NC} Creating test databases..."
for db in pgdog shard_0 shard_1; do
    if psql -c "CREATE DATABASE $db" 2>/dev/null; then
        echo "  ✓ Created database: $db"
        CREATED_DATABASES+=("$db")
    else
        echo "  ⚠ Database $db already exists (skipping)"
    fi
    
    # Grant permissions
    for user in pgdog pgdog1 pgdog2 pgdog3; do
        psql -c "GRANT ALL ON DATABASE $db TO ${user}" 2>/dev/null || true
        psql -c "GRANT ALL ON SCHEMA public TO ${user}" ${db} 2>/dev/null || true
    done
done

# Step 3: Create tables
echo -e "${GREEN}[3/5]${NC} Creating test tables..."
for db in pgdog shard_0 shard_1; do
    echo "  Setting up tables in $db..."
    
    # Sharded tables
    for table in sharded sharded_omni; do
        psql -c "DROP TABLE IF EXISTS ${table}" ${db} -U pgdog 2>/dev/null || true
        psql -c "CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, value TEXT)" ${db} -U pgdog
    done
    
    # Special type tables
    psql -c "CREATE TABLE IF NOT EXISTS sharded_varchar (id_varchar VARCHAR)" ${db} -U pgdog
    psql -c "CREATE TABLE IF NOT EXISTS sharded_uuid (id_uuid UUID PRIMARY KEY)" -d "$db" -U pgdog
    
    # Range/list partitioned tables
    for table in list range; do
        psql -c "CREATE TABLE IF NOT EXISTS sharded_${table} (id BIGINT)" ${db} -U pgdog
    done
    
    psql -c "CREATE TABLE IF NOT EXISTS sharded_list_uuid (id_uuid UUID PRIMARY KEY)" -d "$db" -U pgdog
done

# Step 4: Run schema setup
echo -e "${GREEN}[4/5]${NC} Running schema setup..."
for db in pgdog shard_0 shard_1; do
    echo "  Setting up schema in $db..."
    if [ -f "${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql" ]; then
        psql -f "${SCRIPT_DIR}/../pgdog/src/backend/schema/setup.sql" ${db} -U pgdog 2>/dev/null || {
            echo -e "${YELLOW}  ⚠ Schema setup had warnings for $db${NC}"
        }
    else
        echo -e "${YELLOW}  ⚠ Schema setup file not found${NC}"
    fi
done

# Step 5: Download toxiproxy binaries
echo -e "${GREEN}[5/5]${NC} Setting up toxiproxy..."
pushd ${SCRIPT_DIR} > /dev/null

mkdir -p bin
for bin in toxiproxy-server toxiproxy-cli; do
    if [[ ! -f "bin/${bin}" ]]; then
        echo "  Downloading ${bin}..."
        curl -L "https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/${bin}-${OS}-${ARCH}" > "bin/${bin}" 2>/dev/null
        chmod +x "bin/${bin}"
        echo "  ✓ Downloaded ${bin}"
    else
        echo "  ✓ ${bin} already exists"
    fi
done

popd > /dev/null

# Clear the trap since we succeeded
trap - ERR

# Create a marker file to indicate setup is complete
touch "${SCRIPT_DIR}/.setup_complete"

echo
echo -e "${GREEN}✓ Setup complete!${NC}"
echo "You can now run the integration tests with:"
echo "  ./integration/test"
echo "Or run individual test suites:"
echo "  ./integration/python/run.sh"
echo "  ./integration/go/run.sh"
echo "  etc."