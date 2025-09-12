#!/bin/bash

# Run cargo fmt and capture both stdout and stderr
output=$(cargo fmt 2>&1)
exit_code=$?

# If cargo fmt succeeded (exit code 0), exit silently with 0
if [ $exit_code -eq 0 ]; then
    exit 0
fi

# If cargo fmt failed, output to stderr and exit with 2
echo "$output" >&2
exit 2