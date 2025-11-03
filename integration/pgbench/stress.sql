\set response_size (random(2, 1000000))

-- Generate large response from server.
-- Range: 2 bytes to 1M
SELECT
    string_agg(substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', (random()*61+1)::int, 1), '')
FROM generate_series(1, :response_size);
