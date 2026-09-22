#!/bin/bash
set -e
export PGDOG_BIN=/usr/local/bin/pgdog

# We have to make some changes to account for this being ran in a Firecracker microVM, not a Docker container (as
# this pgdog-base-runtime image was based on that)
printf '127.0.0.1 localhost\n::1 localhost\n' > /etc/hosts
ip link set lo up
mkdir -p /dev/shm && mount -t tmpfs tmpfs /dev/shm
chown -R postgres:postgres /var/lib/postgresql /etc/postgresql /var/log/postgresql
chgrp -R ssl-cert /etc/ssl/private
pg_ctlcluster 18 main start

# This assembles into "Bencher Metric Format"; required because we don't use something like criterion here,
# we have our own custom runtime scripts
json=""
for name in pooler lb sharding; do
    bash benches/perf/run.sh $name > $name.txt 2>&1
    cat $name.txt >&2
    tps=$(awk '/^tps/ {print $3}' $name.txt)
    json+="\"$name\": {\"throughput\": {\"value\": $tps}},"
done

echo "{${json%,}}"
