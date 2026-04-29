-- Creates the parent roles that Vault's IN ROLE clause references.

-- DML role: read/write on application tables
CREATE ROLE dml_role NOLOGIN;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO dml_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO dml_role;
GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO dml_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE,SELECT,UPDATE ON SEQUENCES TO dml_role;

-- DDL role: schema migrations
CREATE ROLE ddl_role NOLOGIN;
GRANT ALL ON SCHEMA public TO ddl_role;
GRANT ALL ON ALL TABLES IN SCHEMA public TO ddl_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ddl_role;

-- Readonly role: analytics/reporting
CREATE ROLE readonly_role NOLOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_role;

-- Create a sample table for testing
CREATE TABLE IF NOT EXISTS demo_items (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO demo_items (name) VALUES ('item-1'), ('item-2'), ('item-3');
