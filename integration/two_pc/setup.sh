#!/bin/bash
export PGPASSWORD=pgdog
export PGHOST=127.0.0.1
export PGPORT=6432
export PGUSER=pgdog
export PGDATABASE=pgdog

psql -c 'CREATE TABLE IF NOT EXISTS omnisharded_two_pc (id BIGINT PRIMARY KEY, value VARCHAR)'
