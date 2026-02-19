-- Test COPY with FK lookup for sharding
-- Run with: psql -h 127.0.0.1 -p 6432 -U pgdog -d pgdog_sharded -f copy_fk_lookup.sql

\echo '=== Setup: Creating FK tables ==='

DROP TABLE IF EXISTS public.copy_orders, public.copy_users;

CREATE TABLE public.copy_users (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL
);

CREATE TABLE public.copy_orders (
    id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES public.copy_users(id)
);

\echo '=== Inserting users with sharding key ==='

INSERT INTO public.copy_users (id, customer_id)
SELECT i, i * 100 + (i % 17)
FROM generate_series(1, 100) AS i;

\echo '=== COPY orders via FK lookup (text format) ==='

COPY public.copy_orders (id, user_id) FROM STDIN;
10	1
20	2
30	3
40	4
50	5
60	6
70	7
80	8
90	9
100	10
\.

\echo '=== Verify: Count orders ==='
SELECT COUNT(*) AS order_count FROM public.copy_orders;

\echo '=== Verify: Sample data ==='
SELECT o.id AS order_id, o.user_id, u.customer_id
FROM public.copy_orders o
JOIN public.copy_users u ON o.user_id = u.id
ORDER BY o.id
LIMIT 10;

\echo '=== Cleanup ==='
DROP TABLE IF EXISTS public.copy_orders, public.copy_users;

\echo '=== DONE ==='
