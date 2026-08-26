\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS bench_schema CASCADE;
DROP PUBLICATION IF EXISTS bench_schema;

CREATE SCHEMA bench_schema;

CREATE TYPE bench_schema.entity_state AS ENUM ('new', 'active', 'archived');
CREATE DOMAIN bench_schema.positive_amount AS NUMERIC(18, 6) CHECK (VALUE >= 0);
CREATE TYPE bench_schema.address AS (line1 TEXT, city TEXT, postcode TEXT);

CREATE TABLE bench_schema.tenants (
    id          BIGSERIAL   PRIMARY KEY,
    tenant_id   BIGINT      NOT NULL,
    name        TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bench_schema.events (
    id          BIGSERIAL   NOT NULL,
    tenant_id   BIGINT      NOT NULL,
    state       bench_schema.entity_state NOT NULL DEFAULT 'new',
    created_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE bench_schema.events_2024 PARTITION OF bench_schema.events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE bench_schema.events_2025 PARTITION OF bench_schema.events
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX events_tenant_idx ON bench_schema.events (tenant_id, created_at);

CREATE TABLE bench_schema.legacy_tenants (
    id          SERIAL      PRIMARY KEY,
    tenant_id   BIGINT      NOT NULL,
    name        TEXT        NOT NULL DEFAULT ''
);

SELECT format($f$
    CREATE TABLE bench_schema.entity_%1$s (
        id          %2$s        PRIMARY KEY,
        tenant_id   BIGINT      NOT NULL,
        owner_id    %3$s,
        state       bench_schema.entity_state NOT NULL DEFAULT 'new',
        amount      bench_schema.positive_amount NOT NULL DEFAULT 0,
        location    bench_schema.address,
        name        TEXT        NOT NULL DEFAULT '',
        payload     JSONB       NOT NULL DEFAULT '{}',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT entity_%1$s_name_length CHECK (length(name) < 256)
    )
$f$,
    i,
    CASE WHEN i % 2 = 0 THEN 'BIGSERIAL' ELSE 'SERIAL' END,
    CASE WHEN i % 2 = 0 THEN 'BIGINT' ELSE 'INTEGER' END)
FROM generate_series(1, :num_tables) AS i \gexec

SELECT format(
    'ALTER TABLE bench_schema.entity_%1$s ADD CONSTRAINT entity_%1$s_owner_fk '
    'FOREIGN KEY (owner_id) REFERENCES bench_schema.%2$s (id)',
    i,
    CASE WHEN i % 2 = 0 THEN 'tenants' ELSE 'legacy_tenants' END)
FROM generate_series(1, :num_tables) AS i \gexec

SELECT format(
    'CREATE INDEX entity_%1$s_tenant_idx ON bench_schema.entity_%1$s (tenant_id, created_at)', i)
FROM generate_series(1, :num_tables) AS i \gexec

SELECT format(
    'CREATE INDEX entity_%1$s_name_idx ON bench_schema.entity_%1$s (name)', i)
FROM generate_series(1, :num_tables) AS i \gexec

SELECT format($f$
    CREATE UNIQUE INDEX entity_%1$s_payload_idx
        ON bench_schema.entity_%1$s ((payload ->> 'key'))
$f$, i)
FROM generate_series(1, :num_tables) AS i \gexec

SELECT format($f$
    COMMENT ON TABLE bench_schema.entity_%1$s IS 'benchmark entity table %1$s'
$f$, i)
FROM generate_series(1, :num_tables) AS i \gexec

CREATE VIEW bench_schema.active_entities AS
    SELECT id, tenant_id, name, amount
    FROM bench_schema.entity_1
    WHERE state = 'active';

CREATE PUBLICATION bench_schema FOR TABLES IN SCHEMA bench_schema;
