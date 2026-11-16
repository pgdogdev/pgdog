BEGIN;

SET LOCAL client_min_messages = warning;

SELECT pg_advisory_xact_lock(hashtext('pgdog.resharding'));

CREATE SCHEMA IF NOT EXISTS pgdog;

CREATE TABLE IF NOT EXISTS pgdog.resharding (
    id UUID PRIMARY KEY,
    source_db TEXT NOT NULL,
    destination_db TEXT NOT NULL,
    publication TEXT NOT NULL,
    command JSONB NOT NULL,
    slot_name TEXT NOT NULL,
    slot_existed BOOLEAN NOT NULL,
    stage TEXT NOT NULL,
    config JSONB NOT NULL,
    instance_id TEXT NOT NULL,
    run_id UUID NOT NULL,
    task_id BIGINT NOT NULL,
    format_version INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    resumed_at TIMESTAMPTZ,
    UNIQUE (source_db, destination_db, publication)
);

COMMIT;
