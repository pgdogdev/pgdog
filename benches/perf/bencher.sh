#!/bin/bash
set -e
export PGDOG_BIN=/usr/local/bin/pgdog

SEED=${BENCH_SEED:-1}

echo 0 > /proc/sys/kernel/randomize_va_space

# Microbench run?
if [[ $1 == "microbench" ]]; then
   taskset -c 1 pgdog-microbench run_benches --nocapture > microbench_out.txt
   awk -f benches/perf/divan_to_bmf.awk microbench_out.txt
   exit 0
fi

# If we're not running a microbenchmark, then we're doing an end-to-end test

# We have to make some changes to account for this being ran in a Firecracker microVM, not a Docker container (as
# this pgdog-base-runtime image was based on that)
printf '127.0.0.1 localhost\n::1 localhost\n' > /etc/hosts
ip link set lo up
mkdir -p /dev/shm && mount -t tmpfs tmpfs /dev/shm
chown -R postgres:postgres /var/lib/postgresql /etc/postgresql /var/log/postgresql
chgrp -R ssl-cert /etc/ssl/private

cores=$(nproc)
echo "cores=${cores} cpu0_siblings=$(cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list 2> /dev/null || echo unknown) seed=${SEED}" >&2

rate_for() {
    if [ -n "${BENCH_RATE:-}" ]; then
        echo "${BENCH_RATE}"
        return
    fi
    case "$1" in
        pooler) echo 17000 ;;
        lb) echo 16000 ;;
        sharding) echo 10000 ;;
        *) echo 10000 ;;
    esac
}

range() {
    if [ "$1" -eq "$2" ]; then
        echo "$1"
    else
        echo "$1-$2"
    fi
}

# This assembles into "Bencher Metric Format"; required because we don't use something like criterion here,
# we have our own custom runtime scripts
json=""
for name in "$@"; do
    # "-wN" suffix runs the suite with N PgDog workers
    suite=${name%-w*}
    workers=1
    if [[ "${name}" == *-w* ]]; then
        workers=${name##*-w}
    fi

    pgdog_cpu=""
    pgbench_cpu=""
    pg_cpu=""
    if [ "${cores}" -ge $((workers + 4)) ]; then
        pgdog_cpu=$(range 1 "${workers}")
        pgbench_cpu=$(range $((workers + 1)) $((workers + 2)))
        pg_cpu=$(range $((workers + 3)) $((cores - 1)))
    elif [ "${cores}" -ge $((workers + 3)) ]; then
        pgdog_cpu=$(range 1 "${workers}")
        pgbench_cpu=$((workers + 1))
        pg_cpu=$(range $((workers + 2)) $((cores - 1)))
    elif [ "${cores}" -gt "${workers}" ]; then
        pgdog_cpu=$(range 0 $((workers - 1)))
        pgbench_cpu=$(range "${workers}" $((cores - 1)))
        pg_cpu=${pgbench_cpu}
    fi
    rate=$(rate_for "${suite}")
    echo "${name}: workers=${workers} rate=${rate} pgdog_cpu=${pgdog_cpu:-unpinned} pgbench_cpu=${pgbench_cpu:-unpinned} pg_cpu=${pg_cpu:-unpinned}" >&2

    pgbench_taskset=""
    pg_taskset=""
    if [ -n "${pgbench_cpu}" ]; then
        pgbench_taskset="taskset -c ${pgbench_cpu}"
        pg_taskset="taskset -c ${pg_cpu}"
    fi

    for phase in warmup measured; do
        seconds=180
        if [ "${phase}" = warmup ]; then
            seconds=30
        fi

        ${pg_taskset} pg_ctlcluster 18 main start
        PGDOG_WORKERS=${workers} PGDOG_CPU=${pgdog_cpu} PGBENCH_TIME=${seconds} PGBENCH_EXTRA="-R ${rate} --random-seed=${SEED}" \
            ${pgbench_taskset} bash benches/perf/run.sh $suite > $name.$phase.txt 2>&1
        pg_ctlcluster 18 main stop
    done

    cat $name.measured.txt >&2
    tps=$(awk '/^tps/ {print $3}' $name.measured.txt)
    txns=$(awk '/^number of transactions actually processed/ {print $NF}' $name.measured.txt)
    cpu=$(awk '/^pgdog_cpu_seconds/ {print $3}' $name.measured.txt)
    ctx=$(awk '/^pgdog_ctx_switches/ {print $3}' $name.measured.txt)
    cpu_per_txn=$(awk -v c=$cpu -v t=$txns 'BEGIN {printf "%.4f", c * 1000000 / t}')
    ctx_per_txn=$(awk -v c=$ctx -v t=$txns 'BEGIN {printf "%.6f", c / t}')
    json+="\"$name-v3\": {\"throughput\": {\"value\": $tps}, \"cpu-per-txn\": {\"value\": $cpu_per_txn}, \"ctx-switches-per-txn\": {\"value\": $ctx_per_txn}},"
done

echo "{${json%,}}"
