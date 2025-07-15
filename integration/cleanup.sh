#!/bin/bash
# Clean up PgDog integration test environment

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== PgDog Integration Test Cleanup ==="
echo

# Check if we should proceed
if [ "${FORCE_CLEANUP:-0}" != "1" ] && [ "${CLEANUP:-0}" != "1" ]; then
    echo -e "${YELLOW}Warning: This will remove all test databases and users!${NC}"
    read -p "Are you sure you want to continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cleanup cancelled."
        exit 0
    fi
fi

# Function to safely execute PostgreSQL commands
safe_psql() {
    PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "$1" 2>/dev/null || true
}

# Step 1: Kill any running PgDog processes
echo -e "${GREEN}[1/6]${NC} Stopping PgDog processes..."
if pgrep -f "pgdog" > /dev/null; then
    pkill -f "pgdog" || true
    sleep 1
    # Force kill if still running
    pkill -9 -f "pgdog" 2>/dev/null || true
    echo "  ✓ Stopped PgDog processes"
else
    echo "  ✓ No PgDog processes running"
fi

# Step 2: Kill any running toxiproxy
echo -e "${GREEN}[2/6]${NC} Stopping toxiproxy..."
if pgrep -f "toxiproxy" > /dev/null; then
    pkill -f "toxiproxy" || true
    echo "  ✓ Stopped toxiproxy"
else
    echo "  ✓ No toxiproxy processes running"
fi

# Step 3: Drop test databases
echo -e "${GREEN}[3/6]${NC} Removing test databases..."
for db in pgdog shard_0 shard_1; do
    echo -n "  Dropping $db... "
    safe_psql "DROP DATABASE IF EXISTS $db WITH (FORCE);"
    echo "✓"
done

# Also drop any test_* databases that might have been created
echo -n "  Checking for other test databases... "
TEST_DBS=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -t -c "SELECT datname FROM pg_database WHERE datname LIKE 'test_%';" 2>/dev/null | grep -v '^$' || true)
if [ -n "$TEST_DBS" ]; then
    echo
    for db in $TEST_DBS; do
        echo -n "    Dropping $db... "
        safe_psql "DROP DATABASE IF EXISTS $db WITH (FORCE);"
        echo "✓"
    done
else
    echo "none found"
fi

# Step 4: Drop test users
echo -e "${GREEN}[4/6]${NC} Removing test users..."
for user in pgdog pgdog1 pgdog2 pgdog3; do
    echo -n "  Dropping user $user... "
    safe_psql "DROP USER IF EXISTS $user;"
    echo "✓"
done

# Step 5: Clean up files and directories
echo -e "${GREEN}[5/6]${NC} Cleaning up files..."

# Remove downloaded binaries
if [ -d "${SCRIPT_DIR}/bin" ]; then
    rm -rf "${SCRIPT_DIR}/bin"
    echo "  ✓ Removed toxiproxy binaries"
fi

# Remove setup marker
if [ -f "${SCRIPT_DIR}/.setup_complete" ]; then
    rm -f "${SCRIPT_DIR}/.setup_complete"
    echo "  ✓ Removed setup marker"
fi

# Clean up language-specific artifacts
echo "  Cleaning language-specific files..."

# Python
if [ -d "${SCRIPT_DIR}/python/venv" ]; then
    rm -rf "${SCRIPT_DIR}/python/venv"
    echo "    ✓ Removed Python virtualenv"
fi
find "${SCRIPT_DIR}/python" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "${SCRIPT_DIR}/python" -name "*.pyc" -delete 2>/dev/null || true

# Ruby
if [ -d "${SCRIPT_DIR}/ruby/.bundle" ]; then
    rm -rf "${SCRIPT_DIR}/ruby/.bundle"
    echo "    ✓ Removed Ruby bundle directory"
fi

# Node.js
for dir in "${SCRIPT_DIR}/js"/*; do
    if [ -d "$dir/node_modules" ]; then
        rm -rf "$dir/node_modules"
        echo "    ✓ Removed node_modules in $(basename "$dir")"
    fi
done

# Go
find "${SCRIPT_DIR}/go" -name "go.sum" -delete 2>/dev/null || true

# Remove log files
find "${SCRIPT_DIR}" -name "pgdog.log" -delete 2>/dev/null || true
find "${SCRIPT_DIR}" -name "*.log" -delete 2>/dev/null || true
echo "  ✓ Removed log files"

# Step 6: Clean build artifacts (optional)
echo -e "${GREEN}[6/6]${NC} Optional: Clean build artifacts"
echo "  To clean Rust build artifacts, run: cargo clean"
echo "  (Skipping to preserve build cache)"

# Summary
echo
echo -e "${GREEN}✓ Cleanup complete!${NC}"
echo
echo "The test environment has been reset. To run tests again:"
echo "  ./test"
echo
echo "Note: PostgreSQL itself was not modified or stopped."