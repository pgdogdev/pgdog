\set user_id random(1, 100)
\set order_total random(1, 10000) / 100.0

BEGIN;
SELECT * FROM users WHERE id = :user_id;
INSERT INTO orders (user_id, total, status) VALUES (:user_id, :order_total, 'pending');
SELECT COUNT(*) FROM orders WHERE user_id = :user_id;
UPDATE orders SET status = 'completed' WHERE user_id = :user_id AND status = 'pending' AND id = (SELECT MAX(id) FROM orders WHERE user_id = :user_id);
COMMIT;
