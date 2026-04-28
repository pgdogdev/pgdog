-- description: ClickBench Citus query 4
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT AVG(UserID) FROM hits;
