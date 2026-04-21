\c pgdog1
DROP SCHEMA IF EXISTS copy_data CASCADE;
\c pgdog2
DROP SCHEMA IF EXISTS copy_data CASCADE;
\c pgdog
DROP SCHEMA IF EXISTS copy_data CASCADE;
\c shard_0
DROP SCHEMA IF EXISTS copy_data CASCADE;
\c shard_1
DROP SCHEMA IF EXISTS copy_data CASCADE;
\c pgdog
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;
