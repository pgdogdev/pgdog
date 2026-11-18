-- description: ClickBench Citus query 8
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
