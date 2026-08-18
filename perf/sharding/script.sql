\set id random(1, 30)

SELECT * FROM sharding_bench WHERE id = :id;
