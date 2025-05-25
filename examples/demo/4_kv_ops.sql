INSERT INTO
    kv (data)
VALUES
    '{"fruit": "orange"}' RETURNING *;


SELECT * FROM kv WHERE id = 1;

SELECT * FROM kv;

SELECT * FROM kv ORDER BY id;

SELECT data->>'fruit', count(*) FROM kv GROUP BY 1 ORDER BY 2 DESC;
