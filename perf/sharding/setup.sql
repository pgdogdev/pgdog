DROP TABLE IF EXISTS sharding_bench;
CREATE TABLE sharding_bench (id BIGINT PRIMARY KEY, value TEXT, created_at TIMESTAMPTZ DEFAULT NOW());

INSERT INTO sharding_bench VALUES (1, 'test1');
INSERT INTO sharding_bench VALUES (2, 'test2');
INSERT INTO sharding_bench VALUES (3, 'test3');
INSERT INTO sharding_bench VALUES (4, 'test4');
INSERT INTO sharding_bench VALUES (5, 'test5');
INSERT INTO sharding_bench VALUES (6, 'test6');
INSERT INTO sharding_bench VALUES (7, 'test7');
INSERT INTO sharding_bench VALUES (8, 'test8');
INSERT INTO sharding_bench VALUES (9, 'test9');
INSERT INTO sharding_bench VALUES (10, 'test10');
INSERT INTO sharding_bench VALUES (11, 'test11');
INSERT INTO sharding_bench VALUES (12, 'test12');
INSERT INTO sharding_bench VALUES (13, 'test13');
INSERT INTO sharding_bench VALUES (14, 'test14');
INSERT INTO sharding_bench VALUES (15, 'test15');
