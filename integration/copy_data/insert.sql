\set id random(100000, 1000000)
\set tenant_id random(1, 3)
INSERT INTO copy_data.users (id, email, tenant_id) VALUES (:id, 'test@test.com', :tenant_id);
