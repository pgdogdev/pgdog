-- Large data setup for testing replication (generates ~50-100GB of data)
-- WARNING: This will take a long time to run and use significant disk space

CREATE SCHEMA IF NOT EXISTS copy_data;

-- Helper function to generate random text
CREATE OR REPLACE FUNCTION copy_data.random_text(len INT) RETURNS TEXT AS $$
SELECT string_agg(chr(65 + (random() * 25)::int), '')
FROM generate_series(1, len);
$$ LANGUAGE SQL;

-- Users table: 5 million rows with ~500 byte JSONB = ~2.5GB
CREATE TABLE IF NOT EXISTS copy_data.users (
   id BIGINT NOT NULL,
   tenant_id BIGINT NOT NULL,
   email VARCHAR NOT NULL,
   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
   settings JSONB NOT NULL DEFAULT '{}'::jsonb,
   PRIMARY KEY(id, tenant_id)
) PARTITION BY HASH(tenant_id);

CREATE TABLE IF NOT EXISTS copy_data.users_0 PARTITION OF copy_data.users
    FOR VALUES WITH (MODULUS 2, REMAINDER 0);

CREATE TABLE IF NOT EXISTS copy_data.users_1 PARTITION OF copy_data.users
    FOR VALUES WITH (MODULUS 2, REMAINDER 1);

TRUNCATE TABLE copy_data.users;

-- Insert users in batches of 100k to avoid memory issues
DO $$
DECLARE
    batch_size INT := 100000;
    total_users INT := 5000000;
    i INT := 0;
BEGIN
    WHILE i < total_users LOOP
        INSERT INTO copy_data.users (id, tenant_id, email, created_at, settings)
        SELECT
            gs.id,
            ((gs.id - 1) % 1000) + 1 AS tenant_id,
            format('user_%s_tenant_%s@example.com', gs.id, ((gs.id - 1) % 1000) + 1) AS email,
            NOW() - (random() * interval '730 days') AS created_at,
            jsonb_build_object(
                'theme', CASE (random() * 3)::int WHEN 0 THEN 'light' WHEN 1 THEN 'dark' ELSE 'auto' END,
                'notifications', (random() > 0.5),
                'preferences', jsonb_build_object(
                    'language', (ARRAY['en', 'es', 'fr', 'de', 'ja', 'zh'])[floor(random() * 6 + 1)::int],
                    'timezone', (ARRAY['UTC', 'America/New_York', 'Europe/London', 'Asia/Tokyo'])[floor(random() * 4 + 1)::int],
                    'bio', copy_data.random_text(200),
                    'address', copy_data.random_text(100),
                    'phone', copy_data.random_text(20),
                    'metadata', copy_data.random_text(100)
                )
            ) AS settings
        FROM generate_series(i + 1, LEAST(i + batch_size, total_users)) AS gs(id);

        i := i + batch_size;
        RAISE NOTICE 'Inserted % users', i;
    END LOOP;
END $$;

-- Orders table: 20 million rows ~1.5GB
DROP TABLE IF EXISTS copy_data.order_items;
DROP TABLE IF EXISTS copy_data.orders;

CREATE TABLE copy_data.orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ,
    notes TEXT
);

CREATE TABLE copy_data.order_items (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    product_name TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    quantity INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ
);

-- Insert orders in batches
DO $$
DECLARE
    batch_size INT := 100000;
    total_orders INT := 20000000;
    i INT := 0;
BEGIN
    WHILE i < total_orders LOOP
        INSERT INTO copy_data.orders (user_id, tenant_id, amount, created_at, refunded_at, notes)
        SELECT
            (floor(random() * 5000000) + 1)::bigint AS user_id,
            (floor(random() * 1000) + 1)::bigint AS tenant_id,
            ROUND((10 + random() * 990)::numeric, 2)::float8 AS amount,
            NOW() - (random() * INTERVAL '730 days') AS created_at,
            CASE WHEN random() < 0.05 THEN NOW() - (random() * INTERVAL '365 days') ELSE NULL END AS refunded_at,
            copy_data.random_text(50) AS notes
        FROM generate_series(1, batch_size);

        i := i + batch_size;
        RAISE NOTICE 'Inserted % orders', i;
    END LOOP;
END $$;

-- Insert order_items: ~60 million rows (3 per order avg) ~6GB with product names
DO $$
DECLARE
    batch_size INT := 100000;
    total_items INT := 60000000;
    i INT := 0;
BEGIN
    WHILE i < total_items LOOP
        INSERT INTO copy_data.order_items (user_id, tenant_id, order_id, product_name, amount, quantity, created_at, refunded_at)
        SELECT
            (floor(random() * 5000000) + 1)::bigint AS user_id,
            (floor(random() * 1000) + 1)::bigint AS tenant_id,
            (floor(random() * 20000000) + 1)::bigint AS order_id,
            'Product ' || copy_data.random_text(30) AS product_name,
            ROUND((5 + random() * 195)::numeric, 2)::float8 AS amount,
            (floor(random() * 5) + 1)::int AS quantity,
            NOW() - (random() * INTERVAL '730 days') AS created_at,
            CASE WHEN random() < 0.05 THEN NOW() - (random() * INTERVAL '365 days') ELSE NULL END AS refunded_at
        FROM generate_series(1, batch_size);

        i := i + batch_size;
        RAISE NOTICE 'Inserted % order_items', i;
    END LOOP;
END $$;

-- Log actions: 300 million rows with larger action data ~30GB
DROP TABLE IF EXISTS copy_data.log_actions;
CREATE TABLE copy_data.log_actions(
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT,
    user_id BIGINT,
    action VARCHAR(50),
    details TEXT,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
DECLARE
    batch_size INT := 500000;
    total_logs INT := 300000000;
    i INT := 0;
BEGIN
    WHILE i < total_logs LOOP
        INSERT INTO copy_data.log_actions (tenant_id, user_id, action, details, ip_address, user_agent, created_at)
        SELECT
            CASE WHEN random() < 0.1 THEN NULL ELSE (floor(random() * 1000) + 1)::bigint END AS tenant_id,
            (floor(random() * 5000000) + 1)::bigint AS user_id,
            (ARRAY['login', 'logout', 'click', 'purchase', 'view', 'error', 'search', 'update', 'delete', 'create'])[
                floor(random() * 10 + 1)::int
            ] AS action,
            copy_data.random_text(50) AS details,
            format('%s.%s.%s.%s', (random()*255)::int, (random()*255)::int, (random()*255)::int, (random()*255)::int) AS ip_address,
            'Mozilla/5.0 ' || copy_data.random_text(30) AS user_agent,
            NOW() - (random() * INTERVAL '730 days') AS created_at
        FROM generate_series(1, batch_size);

        i := i + batch_size;
        RAISE NOTICE 'Inserted % log_actions', i;
    END LOOP;
END $$;

-- With identity: 50 million rows ~2GB
DROP TABLE IF EXISTS copy_data.with_identity;
CREATE TABLE copy_data.with_identity(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    tenant_id BIGINT NOT NULL,
    data TEXT
);

DO $$
DECLARE
    batch_size INT := 100000;
    total_rows INT := 50000000;
    i INT := 0;
BEGIN
    WHILE i < total_rows LOOP
        INSERT INTO copy_data.with_identity (tenant_id, data)
        SELECT
            (floor(random() * 1000) + 1)::bigint,
            copy_data.random_text(20)
        FROM generate_series(1, batch_size);

        i := i + batch_size;
        RAISE NOTICE 'Inserted % with_identity rows', i;
    END LOOP;
END $$;

-- Create indexes after data load for better performance
CREATE INDEX IF NOT EXISTS idx_orders_user_tenant ON copy_data.orders(user_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON copy_data.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_log_actions_tenant ON copy_data.log_actions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_log_actions_created ON copy_data.log_actions(created_at);

-- Analyze tables for query planner
ANALYZE copy_data.users;
ANALYZE copy_data.orders;
ANALYZE copy_data.order_items;
ANALYZE copy_data.log_actions;
ANALYZE copy_data.with_identity;

-- Show table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) as table_size
FROM pg_tables
WHERE schemaname = 'copy_data'
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

DROP PUBLICATION IF EXISTS pgdog;
CREATE PUBLICATION pgdog FOR TABLES IN SCHEMA copy_data;
