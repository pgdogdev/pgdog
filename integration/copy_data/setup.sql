CREATE SCHEMA IF NOT EXISTS copy_data;

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

CREATE TABLE IF NOT EXISTS copy_data.orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ
);

-- TODO: remove the surrogate `id` column once pgdog tolerates tables whose
-- replica identity is REPLICA IDENTITY FULL or a composite unique key.  It
-- exists here only so Table::valid() passes during COPY_DATA (see
-- docs/FIX_ISSUE_914.md, Fix 2).
CREATE TABLE IF NOT EXISTS copy_data.order_items (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL REFERENCES copy_data.orders(id),
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS copy_data.log_actions(
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT,
    action VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE copy_data.with_identity(
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS identity,
    tenant_id BIGINT NOT NULL
);


-- Omni (non-sharded) tables: no tenant_id column.
CREATE TABLE IF NOT EXISTS copy_data.countries (
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS copy_data.currencies (
    id BIGSERIAL PRIMARY KEY,
    code VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    symbol VARCHAR
);

CREATE TABLE IF NOT EXISTS copy_data.categories (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    parent_id INT
);

CREATE TABLE copy_data.settings (
    id BIGSERIAL PRIMARY KEY,
    setting_name TEXT NOT NULL UNIQUE,
    setting_value TEXT NOT NULL
);

DROP PUBLICATION IF EXISTS pgdog;
CREATE PUBLICATION pgdog FOR TABLES IN SCHEMA copy_data;


INSERT INTO copy_data.users (id, tenant_id, email, created_at, settings)
SELECT
    gs.id,
    ((gs.id - 1) % 20) + 1 AS tenant_id,  -- distribute across 20 tenants
    format('user_%s_tenant_%s@example.com', gs.id, ((gs.id - 1) % 20) + 1) AS email,
    NOW() - (random() * interval '365 days') AS created_at,  -- random past date
    jsonb_build_object(
        'theme', CASE (random() * 3)::int
                    WHEN 0 THEN 'light'
                    WHEN 1 THEN 'dark'
                    ELSE 'auto'
                    END,
        'notifications', (random() > 0.5)
    ) AS settings
FROM generate_series(1, 10000) AS gs(id);


WITH u AS (
  -- Pull the 10k users we inserted earlier
  SELECT id AS user_id, tenant_id
  FROM copy_data.users
  WHERE id BETWEEN 1 AND 10000
  ORDER BY id
),
orders_base AS (
  -- One order per user (10k orders), deterministic order_id = user_id
  SELECT
      u.user_id AS order_id,
      u.user_id,
      u.tenant_id,
      -- random created_at in last 365 days
      NOW() - (random() * INTERVAL '365 days') AS created_at,
      -- ~10% refunded
      CASE WHEN random() < 0.10
           THEN NOW() - (random() * INTERVAL '180 days')
           ELSE NULL
      END AS refunded_at
  FROM u
),
items_raw AS (
  -- 1–5 items per order, random amounts $5–$200
  SELECT
      ob.order_id,
      ob.user_id,
      ob.tenant_id,
      -- skew item counts 1..5 (uniform)
      gs.i AS item_index,
      -- random item amount with cents
      ROUND((5 + random() * 195)::numeric, 2)::float8 AS item_amount,
      -- item created_at: on/after order created_at by up to 3 hours
      ob.created_at + (random() * INTERVAL '3 hours') AS item_created_at,
      -- if order refunded, item refunded too (optionally jitter within 2 hours)
      CASE WHEN ob.refunded_at IS NOT NULL
           THEN ob.refunded_at + (random() * INTERVAL '2 hours')
           ELSE NULL
      END AS item_refunded_at
  FROM orders_base ob
  CROSS JOIN LATERAL generate_series(1, 1 + (floor(random()*5))::int) AS gs(i)
),
order_totals AS (
  SELECT
      order_id,
      user_id,
      tenant_id,
      MIN(item_created_at) AS created_at,
      -- sum of item amounts per order
      ROUND(SUM(item_amount)::numeric, 2)::float8 AS order_amount,
      -- carry refund state from items_raw (same per order)
      MAX(item_refunded_at) AS refunded_at
  FROM items_raw
  GROUP BY order_id, user_id, tenant_id
),
ins_orders AS (
  INSERT INTO copy_data.orders (id, user_id, tenant_id, amount, created_at, refunded_at)
  SELECT
      ot.order_id,        -- id = user_id = 1..10000
      ot.user_id,
      ot.tenant_id,
      ot.order_amount,
      ot.created_at,
      ot.refunded_at
  FROM order_totals ot
  RETURNING id
)
INSERT INTO copy_data.order_items (user_id, tenant_id, order_id, amount, created_at, refunded_at)
SELECT
    ir.user_id,
    ir.tenant_id,
    ir.order_id,
    ir.item_amount,
    ir.item_created_at,
    ir.item_refunded_at
FROM items_raw ir;

INSERT INTO copy_data.log_actions (tenant_id, action)
SELECT
    (floor(random() * 10000) + 1)::bigint AS tenant_id,
    (ARRAY['login', 'logout', 'click', 'purchase', 'view', 'error'])[
        floor(random() * 6 + 1)::int
    ] AS action
FROM generate_series(1, 10000);


INSERT INTO copy_data.with_identity (tenant_id)
SELECT floor(random() * 10000)::bigint FROM generate_series(1, 10000);

INSERT INTO copy_data.countries (code, name) VALUES
    ('US', 'United States'), ('GB', 'United Kingdom'), ('DE', 'Germany'),
    ('FR', 'France'), ('JP', 'Japan'), ('CA', 'Canada'),
    ('AU', 'Australia'), ('BR', 'Brazil'), ('IN', 'India'), ('CN', 'China');

INSERT INTO copy_data.currencies (code, name, symbol) VALUES
    ('USD', 'US Dollar', '$'), ('EUR', 'Euro', '€'), ('GBP', 'British Pound', '£'),
    ('JPY', 'Japanese Yen', '¥'), ('CAD', 'Canadian Dollar', 'C$'),
    ('AUD', 'Australian Dollar', 'A$'), ('BRL', 'Brazilian Real', 'R$'),
    ('INR', 'Indian Rupee', '₹'), ('CNY', 'Chinese Yuan', '¥'), ('CHF', 'Swiss Franc', 'Fr');

INSERT INTO copy_data.categories (name, parent_id) VALUES
    ('Electronics', NULL), ('Clothing', NULL), ('Books', NULL),
    ('Home & Garden', NULL), ('Sports', NULL);
INSERT INTO copy_data.categories (name, parent_id) VALUES
    ('Phones', 1), ('Laptops', 1), ('Shirts', 2), ('Pants', 2), ('Fiction', 3);
