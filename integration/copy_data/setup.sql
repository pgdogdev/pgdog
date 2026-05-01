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

CREATE TABLE IF NOT EXISTS copy_data.order_items (
    user_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL REFERENCES copy_data.orders(id),
    amount DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refunded_at TIMESTAMPTZ
);

-- No primary key: replica identity is carried by all columns.
ALTER TABLE copy_data.order_items REPLICA IDENTITY FULL;

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


-- Table used to verify unchanged-TOAST replication.
--
-- `body` is forced to out-of-line EXTERNAL storage so that any UPDATE which
-- does not touch `body` will emit a 'u' marker for that column in the logical
-- replication stream. The replication subscriber must NOT write an empty string
-- into `body` when it sees that marker — it must issue a filtered UPDATE that
-- skips `body` entirely.
CREATE TABLE IF NOT EXISTS copy_data.posts (
    id        BIGINT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    title     TEXT NOT NULL,
    body      TEXT NOT NULL
);

ALTER TABLE copy_data.posts ALTER COLUMN body SET STORAGE EXTERNAL;

INSERT INTO copy_data.posts (id, tenant_id, title, body)
SELECT
    gs.id,
    ((gs.id - 1) % 20) + 1,
    format('title_%s', gs.id),
    -- ~32 KB per row: large enough to guarantee out-of-line TOAST storage.
    repeat(md5(gs.id::text), 1024)
FROM generate_series(1, 50) AS gs(id);


-- Sharded table with no primary key and REPLICA IDENTITY FULL.
-- Rows are uniquely identifiable by (tenant_id, seq) for controlled UPDATE/DELETE testing.
-- seq 1..50 will be UPDATEd (label set to 'updated_N'); seq 51..100 will be DELETEd.
--
-- `body` is forced to EXTERNAL storage so that the seq 1..50 UPDATE (which only
-- changes `label`) produces 'u' in NEW for `body`, exercising the subscriber
-- slow-path. PG materialises OLD fully inline (toast_flatten_tuple) so OLD carries
-- all 4 columns; NEW carries 3 present + 1 'u'. The slow path must bind only the 3
-- present columns (not all 4 from OLD) or PG rejects with a param-count mismatch.
CREATE TABLE IF NOT EXISTS copy_data.full_identity_events (
    tenant_id BIGINT  NOT NULL,
    seq       INT     NOT NULL,
    label     VARCHAR NOT NULL,
    body      TEXT    NOT NULL
);

ALTER TABLE copy_data.full_identity_events REPLICA IDENTITY FULL;
-- Force out-of-line storage: any UPDATE that leaves `body` unchanged emits 'u' in NEW.
ALTER TABLE copy_data.full_identity_events ALTER COLUMN body SET STORAGE EXTERNAL;

INSERT INTO copy_data.full_identity_events (tenant_id, seq, label, body)
SELECT
    ((gs.id - 1) % 10) + 1         AS tenant_id,
    gs.id                           AS seq,
    'event_' || gs.id               AS label,
    repeat(md5(gs.id::text), 1024)  AS body  -- ~32 KB per row
FROM generate_series(1, 100) AS gs(id);

-- Omni (non-sharded) table with REPLICA IDENTITY FULL.
-- The standalone unique index is PostData and is not copied to the destination
-- by schema-sync pre-data. run.sh creates it explicitly on each destination shard
-- after schema-sync and before data-sync, satisfying tables_missing_unique_index().
-- 'click' row will be UPDATEd (label set to 'Click Updated') during the test run.
CREATE TABLE IF NOT EXISTS copy_data.event_types (
    code        VARCHAR NOT NULL,
    label       VARCHAR NOT NULL,
    description TEXT
);

CREATE UNIQUE INDEX ON copy_data.event_types (code);
ALTER TABLE copy_data.event_types REPLICA IDENTITY FULL;

INSERT INTO copy_data.event_types (code, label, description) VALUES
    ('click',    'Click',    'User clicked an element'),
    ('view',     'View',     'User viewed a page'),
    ('purchase', 'Purchase', 'User made a purchase'),
    ('login',    'Login',    'User logged in'),
    ('logout',   'Logout',   'User logged out'),
    -- NULL description: exercises IS NOT DISTINCT FROM NULL in the FULL identity WHERE clause.
    -- A plain = predicate would silently skip this row on UPDATE/DELETE.
    ('null_desc', 'Null Description', NULL);