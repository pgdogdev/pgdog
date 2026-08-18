\set ON_ERROR_STOP on

-- Run from a maintenance database so the fixture databases can be replaced.
\connect postgres

-- Disconnect clients before dropping the test databases. This keeps the setup
-- compatible with PostgreSQL versions that predate DROP DATABASE ... FORCE.
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname IN (
    'pgdog',
    'pgdog1',
    'pgdog2',
    'pgdog3',
    'shard_0',
    'shard_1',
    'shard_2',
    'shard_3'
)
AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS pgdog;
DROP DATABASE IF EXISTS pgdog1;
DROP DATABASE IF EXISTS pgdog2;
DROP DATABASE IF EXISTS pgdog3;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
DROP DATABASE IF EXISTS shard_2;
DROP DATABASE IF EXISTS shard_3;

-- PostgreSQL does not support CREATE ROLE IF NOT EXISTS. Generate CREATE ROLE
-- statements only for roles that are missing, then normalize their attributes.
SELECT format('CREATE ROLE %I', role_name)
FROM (
    VALUES ('pgdog'), ('pgdog1'), ('pgdog2'), ('pgdog3')
) AS roles(role_name)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = role_name
)
\gexec

ALTER ROLE pgdog LOGIN SUPERUSER PASSWORD 'pgdog';
ALTER ROLE pgdog1 LOGIN SUPERUSER PASSWORD 'pgdog';
ALTER ROLE pgdog2 LOGIN SUPERUSER PASSWORD 'pgdog';
ALTER ROLE pgdog3 LOGIN SUPERUSER PASSWORD 'pgdog';

-- GitHub-hosted runners connect as this role during setup.
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', current_user, 'pgdog')
WHERE current_user = 'runner'
\gexec

CREATE DATABASE pgdog;
CREATE DATABASE pgdog1;
CREATE DATABASE pgdog2;
CREATE DATABASE pgdog3;

GRANT ALL PRIVILEGES ON DATABASE pgdog TO pgdog, pgdog1, pgdog2, pgdog3;

\connect pgdog

GRANT ALL PRIVILEGES ON SCHEMA public TO pgdog, pgdog1, pgdog2, pgdog3;

SET ROLE pgdog;

CREATE TABLE sharded (
    id BIGINT PRIMARY KEY,
    value TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    enabled BOOLEAN DEFAULT false,
    user_id BIGINT,
    region_id INTEGER DEFAULT 10,
    country_id SMALLINT DEFAULT 5,
    options JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE sharded_omni (
    id BIGINT PRIMARY KEY,
    value TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    enabled BOOLEAN DEFAULT false,
    user_id BIGINT,
    region_id INTEGER DEFAULT 10,
    country_id SMALLINT DEFAULT 5,
    options JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE sharded_varchar (
    id_varchar VARCHAR
);

CREATE TABLE sharded_uuid (
    id_uuid UUID PRIMARY KEY
);

CREATE TABLE sharded_list_deprecated (
    id BIGINT
);

CREATE TABLE sharded_range_deprecated (
    id BIGINT
);

CREATE TABLE sharded_list (
    id BIGINT
);

CREATE TABLE sharded_range (
    id BIGINT
);

CREATE TABLE sharded_mapping_hierarchy (
    id BIGINT
);

CREATE TABLE sharded_list_uuid (
    id_uuid UUID PRIMARY KEY
);

RESET ROLE;
\connect postgres

-- The shard databases use the same fixtures, so clone the populated database
-- instead of duplicating the table definitions for every shard.
CREATE DATABASE shard_0 TEMPLATE pgdog;
CREATE DATABASE shard_1 TEMPLATE pgdog;
CREATE DATABASE shard_2 TEMPLATE pgdog;
CREATE DATABASE shard_3 TEMPLATE pgdog;

GRANT ALL PRIVILEGES ON DATABASE shard_0 TO pgdog, pgdog1, pgdog2, pgdog3;
GRANT ALL PRIVILEGES ON DATABASE shard_1 TO pgdog, pgdog1, pgdog2, pgdog3;
GRANT ALL PRIVILEGES ON DATABASE shard_2 TO pgdog, pgdog1, pgdog2, pgdog3;
GRANT ALL PRIVILEGES ON DATABASE shard_3 TO pgdog, pgdog1, pgdog2, pgdog3;
