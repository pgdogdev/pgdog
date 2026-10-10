-- description: ClickBench Citus query 24
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
