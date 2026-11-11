BEGIN;

SELECT pg_advisory_xact_lock(hashtext('pgdog.resharding'));

CREATE SCHEMA IF NOT EXISTS pgdog;

CREATE TABLE IF NOT EXISTS pgdog.resharding (
    source_db TEXT NOT NULL,
    destination_db TEXT NOT NULL,
    publication TEXT NOT NULL,
    command TEXT NOT NULL,
    command_flags JSONB NOT NULL,
    slot_name TEXT NOT NULL,
    slot_existed BOOLEAN NOT NULL,
    stage TEXT NOT NULL,
    config JSONB NOT NULL,
    format_version INTEGER NOT NULL,
    PRIMARY KEY (source_db, destination_db, publication)
);

COMMIT;
