-- description: ClickBench Citus query 6
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
