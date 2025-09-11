\set id (1021 * random(1, 10000000))

-- In a transaction.
BEGIN;
INSERT INTO sharded_2pc (id, value) VALUES (:id, 'some value') RETURNING *;
SELECT * FROM sharded_2pc WHERE id = :id AND value = 'some value';
UPDATE sharded_2pc SET value = 'another value' WHERE id = :id;
DELETE FROM sharded_2pc WHERE id = :id AND value = 'another value';
ROLLBACK;

-- Outside a transaction.
INSERT INTO sharded_2pc (id, value) VALUES (:id, 'some value') RETURNING *;
SELECT * FROM sharded_2pc WHERE id = :id AND value = 'some value';
UPDATE sharded_2pc SET value = 'another value' WHERE id = :id;
DELETE FROM sharded_2pc WHERE id = :id;
