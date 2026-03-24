CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE USER pgdog WITH PASSWORD 'pgdog' SUPERUSER;

CREATE DATABASE tenant_template OWNER pgdog;

\c tenant_template

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    total NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);

INSERT INTO users (name, email)
SELECT
    'user_' || i,
    'user_' || i || '@example.com'
FROM generate_series(1, 100) AS i;

INSERT INTO orders (user_id, total, status)
SELECT
    (random() * 99 + 1)::int,
    (random() * 1000)::numeric(10,2),
    (ARRAY['pending', 'completed', 'cancelled'])[floor(random() * 3 + 1)::int]
FROM generate_series(1, 1000) AS i;
