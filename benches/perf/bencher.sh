#!/bin/bash
set -e
export PGDOG_BIN=/usr/local/bin/pgdog

# Microbench run?
if [[ $1 == "microbench" ]]; then
   pgdog-microbench run_benches --nocapture > microbench_out.txt
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
echo "cores=${cores} cpu0_siblings=$(cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list 2> /dev/null || echo unknown)" >&2

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

    # Trying to make PgDog the bottleneck by limiting it to a single core
    # in lb / pooler.
    pgdog_cpu=""
    others=""
    if [ "${cores}" -gt "${workers}" ]; then
        pgdog_cpu="0-$((workers - 1))"
        others="taskset -c ${workers}-$((cores - 1))"
    fi
    echo "${name}: workers=${workers} pgdog_cpu=${pgdog_cpu:-unpinned}" >&2

    ${others} pg_ctlcluster 18 main start
    PGDOG_WORKERS=${workers} PGDOG_CPU=${pgdog_cpu} ${others} bash benches/perf/run.sh $suite > $name.txt 2>&1
    pg_ctlcluster 18 main stop

    cat $name.txt >&2
    tps=$(awk '/^tps/ {print $3}' $name.txt)
    txns=$(awk '/^number of transactions actually processed/ {print $NF}' $name.txt)
    cpu=$(awk '/^pgdog_cpu_seconds/ {print $3}' $name.txt)
    ctx=$(awk '/^pgdog_ctx_switches/ {print $3}' $name.txt)
    cpu_per_txn=$(awk -v c=$cpu -v t=$txns 'BEGIN {printf "%.4f", c * 1000000 / t}')
    ctx_per_txn=$(awk -v c=$ctx -v t=$txns 'BEGIN {printf "%.6f", c / t}')
    json+="\"$name\": {\"throughput\": {\"value\": $tps}, \"cpu-per-txn\": {\"value\": $cpu_per_txn}, \"ctx-switches-per-txn\": {\"value\": $ctx_per_txn}},"
done

echo "{${json%,}}"
