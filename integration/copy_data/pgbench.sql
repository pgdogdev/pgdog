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

-- Clean up everything we created.
DELETE FROM copy_data.orders WHERE id = :order_id;

DELETE FROM copy_data.log_actions WHERE id = :log_id;

DELETE FROM copy_data.users
WHERE id = :user_id AND tenant_id = :tenant_id AND email = 'bench_' || :user_id || '@example.com';
