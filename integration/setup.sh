#!/bin/bash
psql -c "CREATE USER pgdog LOGIN SUPERUSER PASSWORD 'pgdog'"

for db in pgdog shard_0 shard_1; do
    psql -c "CREATE DATABASE $db"
    psql -c "GRANT ALL ON DATABASE $db TO pgdog"
    psql -c "GRANT ALL ON SCHEMA public TO pgdog" ${db}
done

for db in shard_0 shard_1; do
    psql -c 'CREATE TABLE IF NOT EXISTS sharded (id BIGINT, value TEXT)' ${db}
done
