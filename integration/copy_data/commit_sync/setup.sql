-- Source schema + data for the commit_sync test (3 shards).
--
-- Routing is deterministic (hash of tenant_id, 3 shards):
--   tenant 1 -> shard 2  (the fault-injected shard)
--   tenant 2 -> shard 0
--   tenant 3 -> shard 1
--
-- All rows have the same size, so run.sh can compute the byte offsets of the
-- shard 2 copy connection from the row count.

\ir setup_schema.sql

CREATE PUBLICATION commit_sync FOR TABLE commit_sync.items;

INSERT INTO commit_sync.items SELECT g, 1, repeat('x', 512) FROM generate_series(1, 2000) AS g;
INSERT INTO commit_sync.items SELECT g, 2, repeat('x', 512) FROM generate_series(1, 2000) AS g;
INSERT INTO commit_sync.items SELECT g, 3, repeat('x', 512) FROM generate_series(1, 2000) AS g;
