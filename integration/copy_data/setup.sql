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

TRUNCATE TABLE copy_data.users;

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

DROP TABLE copy_data.orders;
CREATE TABLE IF NOT EXISTS copy_data.orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS copy_data.order_items (
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL REFERENCES copy_data.orders(id),
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ
);

-- --- Fix/define schema (safe to run if you're starting fresh) ---
-- Adjust/drop statements as needed if the tables already exist.
TRUNCATE TABLE copy_data.order_items CASCADE;
TRUNCATE TABLE copy_data.orders CASCADE;

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

CREATE TABLE copy_data.log_actions(
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT,
    action VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO copy_data.log_actions (tenant_id, action)
SELECT
    CASE WHEN random() < 0.2 THEN NULL ELSE (floor(random() * 10000) + 1)::bigint END AS tenant_id,
    (ARRAY['login', 'logout', 'click', 'purchase', 'view', 'error'])[
        floor(random() * 6 + 1)::int
    ] AS action
FROM generate_series(1, 10000);

CREATE TABLE copy_data.with_identity(
    id BIGINT GENERATED ALWAYS AS identity,
    tenant_id BIGINT NOT NULL
);

INSERT INTO copy_data.with_identity (tenant_id)
SELECT floor(random() * 10000)::bigint FROM generate_series(1, 10000);

CREATE TABLE IF NOT EXISTS copy_data.address (
    street TEXT,
    city TEXT,
    state VARCHAR,
    country VARCHAR,
    zip TEXT
);

CREATE TABLE IF NOT EXISTS copy_data.customers (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    name VARCHAR NOT NULL,
    address copy_data.address
);

TRUNCATE TABLE copy_data.customers;

INSERT INTO copy_data.customers (tenant_id, name, address)
SELECT
    ((gs.id - 1) % 20) + 1 AS tenant_id,
    CASE WHEN random() < 0.1 THEN format('Customer "%s" Jr.', gs.id)
         ELSE format('Customer %s', gs.id)
    END AS name,
    CASE
        WHEN random() < 0.1 THEN NULL
        WHEN random() < 0.2 THEN ROW(
            format('%s Main St', gs.id),
            NULL,
            NULL,
            NULL,
            NULL
        )::copy_data.address
        WHEN random() < 0.3 THEN ROW(
            NULL,
            (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'])[floor(random() * 5 + 1)::int],
            NULL,
            NULL,
            NULL
        )::copy_data.address
        WHEN random() < 0.4 THEN ROW(
            NULL,
            NULL,
            NULL,
            NULL,
            lpad(floor(random() * 99999)::int::text, 5, '0')
        )::copy_data.address
        WHEN random() < 0.5 THEN ROW(
            format('%s "Main" St', gs.id),
            'New "York"',
            'New York',
            'United States',
            lpad(floor(random() * 99999)::int::text, 5, '0')
        )::copy_data.address
        ELSE ROW(
            format('%s Main St', gs.id),
            (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'])[floor(random() * 5 + 1)::int],
            'CA',
            'United States',
            lpad(floor(random() * 99999)::int::text, 5, '0')
        )::copy_data.address
    END AS address
FROM generate_series(1, 10000) AS gs(id);

INSERT INTO copy_data.customers (tenant_id, name, address)
VALUES (1, 'Customer with " quote', ROW('123 Main St', 'Boston', 'MA', 'United States', '02101')::copy_data.address);

-- Entry with empty street field and trailing quote to trigger edge cases
INSERT INTO copy_data.customers (tenant_id, name, address)
VALUES (2, 'Annapolis Customer', ROW(NULL, 'Annapolis', 'Maryland', 'United States"', NULL)::copy_data.address);

INSERT INTO copy_data.customers (tenant_id, name, address)
VALUES (3, 'Quote in country', ROW(NULL, 'Annapolis', 'Maryland', 'United States', NULL)::copy_data.address);

DROP PUBLICATION IF EXISTS pgdog;
CREATE PUBLICATION pgdog FOR TABLES IN SCHEMA copy_data;
