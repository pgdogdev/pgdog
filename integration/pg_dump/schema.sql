CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped');

CREATE TABLE countries (
    id BIGINT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL
);

CREATE TABLE migrations (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE orders (
    tenant_id BIGINT NOT NULL,
    id BIGINT NOT NULL,
    country_id BIGINT NOT NULL REFERENCES countries (id),
    status order_status NOT NULL DEFAULT 'new',
    amount NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE INDEX orders_status_idx ON orders (tenant_id, status);

CREATE FUNCTION tenant_total(p_tenant_id BIGINT, p_status order_status)
RETURNS NUMERIC
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(SUM(amount), 0)
    FROM orders
    WHERE tenant_id = p_tenant_id AND status = p_status
$$;

CREATE VIEW paid_orders AS
SELECT o.tenant_id, o.id, c.code AS country, o.amount
FROM orders o
JOIN countries c ON c.id = o.country_id
WHERE o.status = 'paid';

INSERT INTO countries (id, code, name) VALUES
    (1, 'US', 'United States'),
    (2, 'DE', 'Germany'),
    (3, 'FR', 'France'),
    (4, 'JP', 'Japan'),
    (5, 'BR', 'Brazil');

INSERT INTO migrations (id, name) VALUES
    (1, '0001_initial'),
    (2, '0002_orders'),
    (3, '0003_paid_orders_view');

INSERT INTO users (id, email)
SELECT g, 'user' || g || '@example.com'
FROM generate_series(1, 20) AS g;
