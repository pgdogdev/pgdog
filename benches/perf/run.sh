#!/bin/bash
set -e
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../integration/common.sh

run_pgdog benches/perf/$1
wait_for_pgdog

# Pin all PgDog threads to the core reserved for it to try to make it the bottleneck
pid=$(cat ${COMMON_DIR}/pgdog.pid)
if [ -n "${PGDOG_CPU:-}" ]; then
    taskset -a -p -c ${PGDOG_CPU} ${pid} > /dev/null
fi

cpu_ticks() {
    awk '{print $14 + $15}' /proc/${pid}/stat
}

# Try to track context switches of PgDog threads to measure contention.
ctx_switches() {
    for task in /proc/${pid}/task/*; do
        echo "${task##*/} $(awk '/^voluntary_ctxt_switches/ {print $2}' ${task}/status 2> /dev/null)"
    done
}

perf_pid=""
if [ -n "${BENCH_PERF_OUT:-}" ]; then
    perf record -q -F 499 -g -p ${pid} -o "${BENCH_PERF_OUT}" > /dev/null 2>&1 &
    perf_pid=$!
fi
perf_stat_pid=""
if [ -n "${BENCH_PERF_STAT_OUT:-}" ]; then
    perf stat -e "${BENCH_PERF_EVENTS:-cycles:u,instructions:u,cycles:k}" -x, -o "${BENCH_PERF_STAT_OUT}" -p ${pid} > /dev/null 2>&1 &
    perf_stat_pid=$!
fi

start=$(cpu_ticks)
ctx_start=$(ctx_switches)
bash ${SCRIPT_DIR}/$1/run.sh
end=$(cpu_ticks)
ctx_end=$(ctx_switches)

if [ -n "${perf_pid}" ]; then
    kill -INT ${perf_pid}
    wait ${perf_pid} || true
fi
echo "pgdog_cpu_seconds = $(awk -v d=$((end - start)) -v hz=$(getconf CLK_TCK) 'BEGIN {print d / hz}')"
echo "pgdog_ctx_switches = $({ echo "${ctx_start}"; echo "--"; echo "${ctx_end}"; } | awk '$1 == "--" {after = 1; next} !after {before[$1] = $2; next} {total += $2 - before[$1]} END {print total + 0}')"
if [ -n "${perf_stat_pid}" ]; then
    kill -INT ${perf_stat_pid}
    wait ${perf_stat_pid} || true
    echo "pgdog_user_cycles = $(awk -F, '$3 ~ /^cycles:u/ {print $1}' "${BENCH_PERF_STAT_OUT}")"
    echo "pgdog_user_instructions = $(awk -F, '$3 ~ /^instructions:u/ {print $1}' "${BENCH_PERF_STAT_OUT}")"
    echo "pgdog_kernel_cycles = $(awk -F, '$3 ~ /^cycles:k/ {print $1}' "${BENCH_PERF_STAT_OUT}")"
fi

stop_pgdog
