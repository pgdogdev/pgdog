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

start=$(cpu_ticks)
ctx_start=$(ctx_switches)
bash ${SCRIPT_DIR}/$1/run.sh
end=$(cpu_ticks)
ctx_end=$(ctx_switches)
echo "pgdog_cpu_seconds = $(awk -v d=$((end - start)) -v hz=$(getconf CLK_TCK) 'BEGIN {print d / hz}')"
echo "pgdog_ctx_switches = $({ echo "${ctx_start}"; echo "--"; echo "${ctx_end}"; } | awk '$1 == "--" {after = 1; next} !after {before[$1] = $2; next} {total += $2 - before[$1]} END {print total + 0}')"

stop_pgdog
