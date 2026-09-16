\set response_size (random(2, 1000000))

-- Generate large responses without evaluating a SQL row per output character.
-- Range: 2 bytes to 1M
SELECT left(
    repeat('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', (:response_size + 61) / 62),
    :response_size
);
