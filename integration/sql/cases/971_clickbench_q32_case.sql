-- description: ClickBench Citus query 32
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
