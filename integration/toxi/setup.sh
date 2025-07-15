#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Look for toxiproxy binaries in the correct location
TOXI_BIN_DIR="${SCRIPT_DIR}/../bin"
TOXI_SERVER="${TOXI_BIN_DIR}/toxiproxy-server"
TOXI_CLI="${TOXI_BIN_DIR}/toxiproxy-cli"

# Check if toxiproxy binaries exist
if [ ! -f "$TOXI_SERVER" ] || [ ! -f "$TOXI_CLI" ]; then
    echo "Error: Toxiproxy binaries not found in $TOXI_BIN_DIR"
    echo "Please run './integration/setup.sh' first to download toxiproxy"
    exit 1
fi

# Make sure binaries are executable
chmod +x "$TOXI_SERVER" "$TOXI_CLI" 2>/dev/null || true

# Check if toxiproxy is already running
if pgrep -f toxiproxy-server > /dev/null 2>&1; then
    echo "Error: Toxiproxy server is already running"
    echo "Please stop the existing toxiproxy-server process before running this setup"
    exit 1
fi

# Start toxiproxy server
echo "Starting toxiproxy server..."
"${TOXI_SERVER}" > /dev/null 2>&1 &
TOXI_PID=$!

# Wait for toxiproxy to start
echo "Waiting for toxiproxy to start..."
for i in {1..10}; do
    if "${TOXI_CLI}" list >/dev/null 2>&1; then
        echo "Toxiproxy is ready"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "Error: Toxiproxy failed to start"
        kill $TOXI_PID 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# Clean up any existing proxies
"${TOXI_CLI}" delete primary 2>/dev/null || true
"${TOXI_CLI}" delete replica 2>/dev/null || true
"${TOXI_CLI}" delete replica2 2>/dev/null || true
"${TOXI_CLI}" delete replica3 2>/dev/null || true

# Create new proxies
echo "Creating toxiproxy endpoints..."
"${TOXI_CLI}" create --listen :5435 --upstream :5432 primary
"${TOXI_CLI}" create --listen :5436 --upstream :5432 replica
"${TOXI_CLI}" create --listen :5437 --upstream :5432 replica2
"${TOXI_CLI}" create --listen :5438 --upstream :5432 replica3

# List all proxies
echo "Toxiproxy setup complete. Active proxies:"
"${TOXI_CLI}" list