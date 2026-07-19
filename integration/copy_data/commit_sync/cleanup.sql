-- Tear down the commit_sync test objects. Shared by setup_schema.sql, setup.sql,
-- and run.sh's cleanup trap. DROP IF EXISTS keeps it a safe no-op on the shards
-- (which have no publication) and on a fresh database.
DROP PUBLICATION IF EXISTS commit_sync;
DROP SCHEMA IF EXISTS commit_sync CASCADE;
