-- description: LIMIT and OFFSET apply to groups merged across shards, not to each shard's groups
-- tags: standard sharded
-- transactional: true

SELECT category, COUNT(*) FROM sql_regression_samples GROUP BY category ORDER BY 2 DESC, 1 LIMIT 1;
SELECT category, COUNT(*) FROM sql_regression_samples GROUP BY category ORDER BY 2 DESC, 1 LIMIT 1 OFFSET 1;
SELECT category, COUNT(*) FROM sql_regression_samples GROUP BY category ORDER BY 2 DESC, 1 OFFSET 2;
SELECT category, SUM(amount) FROM sql_regression_samples GROUP BY 1 ORDER BY 2 DESC, 1 FETCH FIRST 2 ROWS ONLY;
SELECT category, AVG(amount), COUNT(*) FROM sql_regression_samples GROUP BY category ORDER BY 3 DESC, 1 LIMIT 2;
SELECT category, COUNT(*) FROM sql_regression_samples GROUP BY category ORDER BY 2 DESC, 1 LIMIT 1 + 1 OFFSET 2 - 1;
SELECT category, COUNT(*) FROM sql_regression_samples GROUP BY category ORDER BY 2 DESC, 1 LIMIT 1::int OFFSET 1::smallint;
SELECT id FROM sql_regression_samples ORDER BY id LIMIT 2 * 2 OFFSET 3 - 1;
