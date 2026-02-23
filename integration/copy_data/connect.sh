#!/bin/bash

case "$1" in
    source)
        PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15432 -U pgdog -d pgdog
        ;;
    shard_0|0)
        PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15433 -U pgdog -d pgdog1
        ;;
    shard_1|1)
        PGPASSWORD=pgdog psql -h 127.0.0.1 -p 15434 -U pgdog -d pgdog2
        ;;
    *)
        echo "Usage: $0 {source|shard_0|0|shard_1|1}"
        exit 1
        ;;
esac
