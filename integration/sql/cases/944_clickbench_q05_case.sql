-- description: ClickBench Citus query 5
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT COUNT(DISTINCT UserID) FROM hits;
