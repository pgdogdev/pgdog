#!/bin/bash

psql shard_0 -c 'CREATE TABLE IF NOT EXISTS sharded (id BIGINT, value TEXT) PARTITION BY HASH(id)' -U pgdog
psql shard_1 -c 'CREATE TABLE IF NOT EXISTS sharded (id BIGINT, value TEXT) PARTITION BY HASH(id)' -U pgdog

psql shard_0 -c 'CREATE TABLE IF NOT EXISTS sharded_0 PARTITION OF sharded FOR VALUES WITH (modulus 2, remainder 0)' -U pgdog
psql shard_1 -c 'CREATE TABLE IF NOT EXISTS sharded_1 PARTITION OF sharded FOR VALUES WITH (modulus 2, remainder 1)' -U pgdog
