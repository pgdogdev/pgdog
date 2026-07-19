-- Source-only reset for the docker-compose retry test.
-- Each database lives in its own container, so \c cannot cross server boundaries.
-- Destination shards are reset by retry_test.sh via separate psql invocations.
DROP SCHEMA IF EXISTS copy_data CASCADE;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;
\i setup.sql
