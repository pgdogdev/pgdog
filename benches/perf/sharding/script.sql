\set id random(1, 10000)
\set id2 random(10000, 20000)
\set id3 random(20000, 30000)


INSERT INTO sharding_bench (id, value) VALUES (:id, 'test1'), (:id2, 'test3'), (:id3, 'test3');
