-- Schema shared by the source and every destination shard for the commit_sync test.
-- data-sync runs with --skip-schema-sync, so the destination tables must pre-exist;
-- the source is built from the same definition. Created empty: setup.sql layers the
-- publication and source rows on top of this.

\ir cleanup.sql

CREATE SCHEMA commit_sync;

CREATE TABLE commit_sync.items (
    id        BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    payload   TEXT   NOT NULL,
    PRIMARY KEY (id, tenant_id)
);
