-- description: ClickBench Citus query 19
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
