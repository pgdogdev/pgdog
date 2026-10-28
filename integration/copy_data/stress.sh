#!/bin/bash
# Temporary: run retry_test and commit_sync ITERATIONS times each and report every failure.
set -uo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ITERATIONS=${ITERATIONS:-5}
LOG_DIR=${LOG_DIR:-"${SCRIPT_DIR}/stress-logs"}
mkdir -p "${LOG_DIR}"

failed=0
for i in $(seq 1 "${ITERATIONS}"); do
    for test in retry_test commit_sync; do
        log="${LOG_DIR}/${test}_${i}.log"
        start=$(date +%s)
        if bash "${SCRIPT_DIR}/${test}/run.sh" >"${log}" 2>&1; then
            echo "[stress] ${test} ${i}/${ITERATIONS}: PASS ($(( $(date +%s) - start ))s)"
        else
            failed=$((failed + 1))
            echo "[stress] ${test} ${i}/${ITERATIONS}: FAIL ($(( $(date +%s) - start ))s), log: ${log}"
            grep -E "^\[${test}\]|WARN|ERROR" "${log}" | grep -vE 'connection_creation|retrying after' | tail -40
        fi
    done
done

echo "[stress] ${failed} failure(s) in $(( ITERATIONS * 2 )) runs"
[ "${failed}" -eq 0 ]
