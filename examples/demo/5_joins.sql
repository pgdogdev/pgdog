CREATE TABLE kv_meta (
    kv_id BIGINT NOT NULL REFERENCES kv(id),
    created_by VARCHAR NOT NULL,
    admin BOOL NOT NULL DEFAULT false
);

INSERT INTO kv_meta (data_id, created_by) VALUES (1, 'lev') RETURNING *;
INSERT INTO kv_meta (data_id, created_by) VALUES (1, 'bob') RETURNING *;
INSERT INTO kv_meta (data_id, created_by) VALUES (1, 'alice') RETURNING *;


SELECT * FROM data INNER JOIN kv_meta
ON data.id = kv_meta.data_id
WHERE data_id = 1;

SELECT * FROM data INNER JOIN kv_meta
ON data.id = kv_meta.data_id;
