\set tenant_id random(1, 20)
\set user_id (1021 * random(10000, 100000))
\set order_id random(100001, 999999999)
\set log_id random(100001, 999999999)
\set order_amount random(1, 50000) / 100.0

-- Upsert a user for this tenant.
INSERT INTO copy_data.users (id, tenant_id, email, settings)
VALUES (:user_id, :tenant_id, 'bench_' || :user_id || '@example.com', '{"theme":"dark"}')
ON CONFLICT (id, tenant_id) DO UPDATE SET settings = EXCLUDED.settings;

-- Read the user back.
SELECT id, tenant_id, email, created_at FROM copy_data.users
WHERE id = :user_id AND tenant_id = :tenant_id;

-- Insert an order with an explicit large id.
INSERT INTO copy_data.orders (id, user_id, tenant_id, amount)
VALUES (:order_id, :user_id, :tenant_id, :order_amount)
ON CONFLICT (id) DO NOTHING;

-- Read recent orders for this tenant.
SELECT id, user_id, amount, created_at FROM copy_data.orders
WHERE tenant_id = :tenant_id ORDER BY created_at DESC LIMIT 5;

-- Update the user settings.
UPDATE copy_data.users SET settings = jsonb_set(settings, '{last_bench}', to_jsonb(now()::text))
WHERE id = :user_id AND tenant_id = :tenant_id;

-- Log an action with an explicit large id.
INSERT INTO copy_data.log_actions (id, tenant_id, action)
VALUES (:log_id, :tenant_id, 'bench')
ON CONFLICT (id) DO NOTHING;

\set country_id random(1, 10)
\set currency_id random(1, 10)
\set category_id random(1, 10)

-- Upsert a country.
INSERT INTO copy_data.countries (id, code, name)
VALUES (:country_id, 'X' || :country_id, 'Bench Country ' || :country_id)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

-- Read a country.
SELECT id, code, name FROM copy_data.countries WHERE id = :country_id;

-- Update a currency symbol.
UPDATE copy_data.currencies SET symbol = '$' WHERE id = :currency_id;

-- Read a currency.
SELECT id, code, name, symbol FROM copy_data.currencies WHERE id = :currency_id;

-- Upsert a category.
INSERT INTO copy_data.categories (id, name, parent_id)
VALUES (:category_id, 'Bench Category ' || :category_id, NULL)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

-- Read a category.
SELECT id, name, parent_id FROM copy_data.categories WHERE id = :category_id;

-- Clean up everything we created.
DELETE FROM copy_data.orders WHERE id = :order_id;

DELETE FROM copy_data.log_actions WHERE id = :log_id;

DELETE FROM copy_data.users
WHERE id = :user_id AND tenant_id = :tenant_id AND email = 'bench_' || :user_id || '@example.com';

DELETE FROM copy_data.countries WHERE id = :country_id AND code = 'X' || :country_id;

DELETE FROM copy_data.categories WHERE id = :category_id AND name = 'Bench Category ' || :category_id;
