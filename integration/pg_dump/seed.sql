INSERT INTO orders (tenant_id, id, country_id, status, amount)
SELECT
    :tenant_id,
    g,
    g % 5 + 1,
    (ARRAY['new', 'paid', 'shipped']::order_status[])[g % 3 + 1],
    g * 1.25
FROM generate_series(1, :order_count) AS g;
