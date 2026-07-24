#!/bin/bash

echo "Starting PgDog Elixir Integration Tests"
echo "======================================="

# Check if pgdog is running
if ! nc -z 127.0.0.1 6432; then
    echo "Error: PgDog is not running on port 6432"
    echo "Please start pgdog before running tests"
    exit 1
fi

echo "PgDog detected on port 6432"
echo "Running tests..."

mix test --trace

echo "Tests completed"