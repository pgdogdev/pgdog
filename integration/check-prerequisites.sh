#!/bin/bash
# Check prerequisites for running PgDog integration tests

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track overall status
CHECKS_PASSED=true

# Helper functions
print_check() {
    echo -n "Checking $1... "
}

print_ok() {
    echo -e "${GREEN}✓${NC} $1"
}

print_fail() {
    echo -e "${RED}✗${NC} $1"
    CHECKS_PASSED=false
}

print_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

check_command() {
    local cmd=$1
    local package=$2
    print_check "$cmd"
    if command -v "$cmd" &> /dev/null; then
        print_ok "found"
    else
        print_fail "not found. Please install $package"
    fi
}

# Check PostgreSQL
check_postgres() {
    print_check "PostgreSQL"
    
    # Check if psql is available
    if ! command -v psql &> /dev/null; then
        print_fail "psql not found. Please install PostgreSQL client"
        return
    fi
    
    # Try to connect to PostgreSQL
    if PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "SELECT version();" &> /dev/null; then
        # Get PostgreSQL version
        PG_VERSION=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -t -c "SELECT version();" | grep -oE 'PostgreSQL [0-9]+' | cut -d' ' -f2)
        if [ -n "$PG_VERSION" ] && [ "$PG_VERSION" -ge 15 ]; then
            print_ok "running (version $PG_VERSION)"
        else
            print_warn "running (version $PG_VERSION - v17 recommended)"
        fi
    else
        print_fail "cannot connect to PostgreSQL on localhost:5432"
        echo "  Make sure PostgreSQL is running and accessible"
        echo "  Expected: postgres user with password 'postgres'"
    fi
}

# Check if user can create databases
check_postgres_permissions() {
    print_check "PostgreSQL permissions"
    
    if ! command -v psql &> /dev/null; then
        print_fail "skipped (psql not found)"
        return
    fi
    
    # Try to create and drop a test database
    TEST_DB="pgdog_test_$$"
    if PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE $TEST_DB;" &> /dev/null; then
        PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "DROP DATABASE $TEST_DB;" &> /dev/null
        print_ok "can create databases"
    else
        print_fail "cannot create databases. Need superuser or CREATEDB privilege"
    fi
}

# Check ports
check_port() {
    local port=$1
    local service=$2
    print_check "port $port ($service)"
    
    if lsof -i :$port &> /dev/null || netstat -an 2>/dev/null | grep -q ":$port.*LISTEN"; then
        # Port is in use, check if it's PostgreSQL or PgDog
        local process=$(lsof -i :$port 2>/dev/null | grep LISTEN | awk '{print $1}' | head -1)
        if [ "$port" = "5432" ] && [[ "$process" == *"postgres"* ]]; then
            print_ok "PostgreSQL listening"
        elif [ "$port" = "6432" ] && [[ "$process" == *"pgdog"* ]]; then
            print_warn "PgDog already running"
        else
            print_fail "already in use by $process"
        fi
    else
        print_ok "available"
    fi
}

# Check Rust toolchain
check_rust() {
    print_check "Rust toolchain"
    if command -v cargo &> /dev/null; then
        RUST_VERSION=$(cargo --version | cut -d' ' -f2)
        print_ok "found (cargo $RUST_VERSION)"
    else
        print_fail "not found. Please install from https://rustup.rs"
    fi
}

# Check cargo-nextest
check_nextest() {
    print_check "cargo-nextest"
    if cargo nextest --version &> /dev/null 2>&1; then
        NEXTEST_VERSION=$(cargo nextest --version | grep nextest | cut -d' ' -f2)
        print_ok "found (version $NEXTEST_VERSION)"
    else
        print_fail "not found. Install with: cargo install cargo-nextest --version '0.9.78' --locked"
    fi
}

# Check language-specific requirements
check_python() {
    print_check "Python"
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
        print_ok "found (version $PYTHON_VERSION)"
        
        # Check for virtualenv
        print_check "Python virtualenv"
        if python3 -m venv --help &> /dev/null 2>&1; then
            print_ok "available"
        else
            print_warn "not available. Install python3-venv package"
        fi
    else
        print_warn "not found. Python tests will be skipped"
    fi
}

check_ruby() {
    print_check "Ruby"
    if command -v ruby &> /dev/null && command -v bundle &> /dev/null; then
        RUBY_VERSION=$(ruby --version | cut -d' ' -f2)
        print_ok "found (version $RUBY_VERSION)"
    else
        print_warn "not found. Ruby tests will be skipped"
    fi
}

check_go() {
    print_check "Go"
    if command -v go &> /dev/null; then
        GO_VERSION=$(go version | cut -d' ' -f3)
        print_ok "found ($GO_VERSION)"
    else
        print_warn "not found. Go tests will be skipped"
    fi
}

check_node() {
    print_check "Node.js"
    if command -v node &> /dev/null && command -v npm &> /dev/null; then
        NODE_VERSION=$(node --version)
        print_ok "found ($NODE_VERSION)"
    else
        print_warn "not found. JavaScript tests will be skipped"
    fi
}

check_java() {
    print_check "Java"
    if command -v java &> /dev/null; then
        # Try to get Java version
        if java -version &> /dev/null; then
            print_ok "found"
        else
            print_ok "found (version unknown)"
        fi
    else
        print_warn "not found. Java tests will be skipped"
    fi
}

# Check disk space
check_disk_space() {
    print_check "disk space"
    AVAILABLE_MB=$(df -m . | tail -1 | awk '{print $4}')
    if [ "$AVAILABLE_MB" -gt 2048 ]; then
        print_ok "${AVAILABLE_MB}MB available"
    else
        print_warn "${AVAILABLE_MB}MB available (2GB recommended)"
    fi
}

# Main checks
echo "=== PgDog Integration Test Prerequisites ==="
echo

echo "Essential requirements:"
check_postgres
check_postgres_permissions
check_port 5432 "PostgreSQL"
check_port 6432 "PgDog"
check_rust
check_nextest
check_command "psql" "postgresql-client"
check_disk_space

echo
echo "Optional language support:"
check_python
check_ruby
check_go
check_node
check_java

echo
if [ "$CHECKS_PASSED" = true ]; then
    echo -e "${GREEN}All essential checks passed!${NC}"
    echo "You can run the integration tests with: ./integration/test"
    exit 0
else
    echo -e "${RED}Some essential checks failed.${NC}"
    echo "Please fix the issues above before running tests."
    exit 1
fi