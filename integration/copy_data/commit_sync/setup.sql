-- Source schema + data for the COPY_DATA partial-commit reproduction (3 shards).
--
-- Routing is deterministic (hash of tenant_id, 3 shards):
--   tenant 1 -> shard 2  (the fault-injected shard)
--   tenant 2 -> shard 0
--   tenant 3 -> shard 1
--
-- All three shards get the same number of equally-sized rows. The toxic on shard 2 is
-- sized so that shard 2's whole row payload streams through and the connection is severed
-- only at the copy_done handshake. Because the destination COPY defers its commit until
-- every shard has staged, that mid-COPY fault rolls every shard back: no partial commit.

\ir setup_schema.sql

CREATE PUBLICATION commit_sync FOR TABLE commit_sync.items;

INSERT INTO commit_sync.items SELECT g, 1, repeat('x', 512) FROM generate_series(1, 2000) AS g;
INSERT INTO commit_sync.items SELECT g, 2, repeat('x', 512) FROM generate_series(1, 2000) AS g;
INSERT INTO commit_sync.items SELECT g, 3, repeat('x', 512) FROM generate_series(1, 2000) AS g;
