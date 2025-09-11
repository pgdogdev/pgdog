#!/bin/bash
export PGPASSWORD=pgdog
export PGHOST=127.0.0.1
export PGPORT=6432
export PGUSER=pgdog
export PGDATABASE=pgdog

pgbench -f script.sql -c 10 -t 1000 -P 1
