#!/bin/bash
#
# Common utilities for PgDog integration tests
# Scripts using this are expected to define $SCRIPT_DIR correctly.
#
COMMON_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Generate unique test run ID for isolation
TEST_RUN_ID="${TEST_RUN_ID:-$(date +%s)_$$}"
PGDOG_PID=""

# Helper function to get unique test database name
get_test_db_name() {
    local base_name="${1:-test}"
    echo "${base_name}_${TEST_RUN_ID}"
}

# Enhanced wait function with timeout
wait_for_pgdog() {
    local timeout="${1:-30}"
    local elapsed=0
    
    echo "Waiting for PgDog to start..."
    while ! pg_isready -h 127.0.0.1 -p 6432 -U pgdog -d pgdog > /dev/null 2>&1; do
        if [ $elapsed -ge $timeout ]; then
            echo -e "${RED}Timeout waiting for PgDog to start${NC}"
            return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        printf "."
    done
    echo -e " ${GREEN}✓${NC}"
    echo "PgDog is ready"
    return 0
}

# Wait for a specific port to be available
wait_for_port() {
    local port=$1
    local timeout="${2:-30}"
    local elapsed=0
    
    echo -n "Waiting for port $port to be available..."
    while lsof -i :$port > /dev/null 2>&1; do
        if [ $elapsed -ge $timeout ]; then
            echo -e " ${RED}timeout${NC}"
            return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo -e " ${GREEN}✓${NC}"
    return 0
}

# Enhanced PgDog runner with better process management
run_pgdog() {
    local config_file="${1:-integration/pgdog.toml}"
    local users_file="${2:-integration/users.toml}"
    local log_file="${SCRIPT_DIR}/pgdog.log"
    
    # Ensure any existing PgDog is stopped
    stop_pgdog_quiet
    
    # Wait for port to be available
    if ! wait_for_port 6432 10; then
        echo -e "${RED}Port 6432 is in use. Cannot start PgDog.${NC}"
        return 1
    fi
    
    pushd ${COMMON_DIR}/../ > /dev/null
    
    # Build if not already built
    if [ ! -f "target/release/pgdog" ]; then
        echo "Building PgDog..."
        cargo build --release || {
            echo -e "${RED}Failed to build PgDog${NC}"
            popd > /dev/null
            return 1
        }
    fi
    
    # Start PgDog with proper error handling
    echo "Starting PgDog..."
    target/release/pgdog \
        --config "$config_file" \
        --users "$users_file" \
        > "$log_file" 2>&1 &
    
    PGDOG_PID=$!
    popd > /dev/null
    
    # Verify PgDog started successfully
    sleep 1
    if ! kill -0 $PGDOG_PID 2>/dev/null; then
        echo -e "${RED}PgDog failed to start. Check logs:${NC}"
        tail -20 "$log_file"
        return 1
    fi
    
    # Wait for PgDog to be ready
    if ! wait_for_pgdog; then
        echo -e "${RED}PgDog started but is not responding${NC}"
        stop_pgdog
        return 1
    fi
    
    echo -e "${GREEN}PgDog started successfully (PID: $PGDOG_PID)${NC}"
    return 0
}

# Start PgDog with custom ports (for parallel testing)
run_pgdog_custom_port() {
    local pgdog_port="${1:-6432}"
    local config_file="${2:-integration/pgdog.toml}"
    local users_file="${3:-integration/users.toml}"
    
    # Create temporary config with custom port
    local temp_config="/tmp/pgdog_test_${TEST_RUN_ID}.toml"
    sed "s/6432/${pgdog_port}/g" "${COMMON_DIR}/../${config_file}" > "$temp_config"
    
    run_pgdog "$temp_config" "${COMMON_DIR}/../${users_file}"
    local result=$?
    
    # Cleanup temp config on exit
    trap "rm -f $temp_config" EXIT
    
    return $result
}

# Gracefully stop PgDog
stop_pgdog() {
    local log_file="${SCRIPT_DIR:-${COMMON_DIR}}/pgdog.log"
    
    if [ -n "$PGDOG_PID" ] && kill -0 $PGDOG_PID 2>/dev/null; then
        echo -n "Stopping PgDog (PID: $PGDOG_PID)..."
        kill -TERM $PGDOG_PID 2>/dev/null || true
        
        # Wait for process to stop
        local timeout=5
        while kill -0 $PGDOG_PID 2>/dev/null && [ $timeout -gt 0 ]; do
            sleep 1
            timeout=$((timeout - 1))
        done
        
        # Force kill if still running
        if kill -0 $PGDOG_PID 2>/dev/null; then
            kill -9 $PGDOG_PID 2>/dev/null || true
        fi
        echo -e " ${GREEN}✓${NC}"
    else
        # Fallback to killall
        killall -TERM pgdog 2>/dev/null || true
    fi
    
    # Show last 20 lines of log if it exists
    if [ -f "$log_file" ]; then
        echo "Last 20 lines of PgDog log:"
        echo "=========================="
        tail -20 "$log_file"
        echo "=========================="
    fi
    
    PGDOG_PID=""
}

# Stop PgDog quietly (no output)
stop_pgdog_quiet() {
    if [ -n "$PGDOG_PID" ] && kill -0 $PGDOG_PID 2>/dev/null; then
        kill -TERM $PGDOG_PID 2>/dev/null || true
        sleep 1
        kill -9 $PGDOG_PID 2>/dev/null || true
    fi
    killall -TERM pgdog 2>/dev/null || true
    PGDOG_PID=""
}

# Toxiproxy management with better error handling
start_toxi() {
    local toxi_path="${COMMON_DIR}/bin/toxiproxy-server"
    
    if [ ! -f "$toxi_path" ]; then
        echo -e "${RED}toxiproxy-server not found. Run setup.sh first.${NC}"
        return 1
    fi
    
    echo -n "Starting toxiproxy..."
    "$toxi_path" > "${COMMON_DIR}/toxiproxy.log" 2>&1 &
    local toxi_pid=$!
    
    # Wait for toxiproxy to start
    sleep 2
    if kill -0 $toxi_pid 2>/dev/null; then
        echo -e " ${GREEN}✓${NC} (PID: $toxi_pid)"
        return 0
    else
        echo -e " ${RED}failed${NC}"
        return 1
    fi
}

stop_toxi() {
    echo -n "Stopping toxiproxy..."
    killall -TERM toxiproxy-server 2>/dev/null || true
    echo -e " ${GREEN}✓${NC}"
}

# Python virtual environment activation
activate_venv() {
    local venv_path="${COMMON_DIR}/python/venv"
    
    if [ ! -d "$venv_path" ]; then
        echo -e "${YELLOW}Python venv not found. Creating...${NC}"
        pushd "${COMMON_DIR}/python" > /dev/null
        python3 -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt
        popd > /dev/null
    else
        pushd "${COMMON_DIR}/python" > /dev/null
        source venv/bin/activate
        popd > /dev/null
    fi
}

# Error handler for test scripts
on_test_error() {
    local line_no=$1
    local bash_command="$2"
    local exit_code=$3
    
    echo
    echo -e "${RED}Test failed!${NC}"
    echo "  Location: line $line_no"
    echo "  Command: $bash_command"
    echo "  Exit code: $exit_code"
    
    # Show PgDog logs if available
    local log_file="${SCRIPT_DIR:-${COMMON_DIR}}/pgdog.log"
    if [ -f "$log_file" ]; then
        echo
        echo "PgDog logs (last 50 lines):"
        echo "=========================="
        tail -50 "$log_file"
    fi
    
    # Ensure cleanup happens
    stop_pgdog_quiet
    
    exit $exit_code
}

# Setup error handling for test scripts
setup_test_error_handling() {
    set -euo pipefail
    trap 'on_test_error $LINENO "$BASH_COMMAND" $?' ERR
}

# Test result reporting
report_test_result() {
    local test_name="$1"
    local result="$2"
    
    if [ "$result" -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $test_name"
    else
        echo -e "${RED}✗${NC} $test_name"
    fi
}

# Create test database with proper error handling
create_test_database() {
    local db_name="$1"
    local user="${2:-pgdog}"
    
    echo -n "Creating test database $db_name..."
    if PGPASSWORD=pgdog psql -h localhost -p 5432 -U "$user" -c "CREATE DATABASE $db_name" 2>/dev/null; then
        echo -e " ${GREEN}✓${NC}"
        return 0
    else
        echo -e " ${RED}failed${NC}"
        return 1
    fi
}

# Drop test database
drop_test_database() {
    local db_name="$1"
    local user="${2:-pgdog}"
    
    PGPASSWORD=pgdog psql -h localhost -p 5432 -U "$user" -c "DROP DATABASE IF EXISTS $db_name" 2>/dev/null || true
}

# Export useful variables
export TEST_RUN_ID
export -f get_test_db_name
export -f wait_for_pgdog
export -f wait_for_port
export -f report_test_result