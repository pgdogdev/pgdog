\set id (1021 * random(1, 10000000))

-- In a transaction.
BEGIN;
INSERT INTO sharded (id, value) VALUES (:id, 'some value') RETURNING *;
SELECT * FROM sharded WHERE id = :id AND value = 'some value';
UPDATE sharded SET value = 'another value' WHERE id = :id;
DELETE FROM sharded WHERE id = :id AND value = 'another value';
ROLLBACK;

-- Outside a transaction.
INSERT INTO sharded (id, value) VALUES (:id, 'some value') RETURNING *;
SELECT * FROM sharded WHERE id = :id AND value = 'some value';
UPDATE sharded SET value = 'another value' WHERE id = :id;
DELETE FROM sharded WHERE id = :id;
