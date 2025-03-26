#!/bin/bash
if [[ ! -z "$1" ]]; then
    PGPASSWORD=pgdog psql -h 127.0.0.1 -p 6432 -U pgdog pgdog_sharded
else
    PGPASSWORD=pgdog psql -h 127.0.0.1 -p 6432 -U pgdog pgdog
fi
