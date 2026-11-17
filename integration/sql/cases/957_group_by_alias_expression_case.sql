-- description: GROUP BY on a SELECT alias or an expression merges across shards
-- tags: standard sharded
-- transactional: true

SELECT value % 2 AS parity, COUNT(*) FROM sql_regression_samples GROUP BY parity ORDER BY parity;

SELECT COUNT(*), lower(label) AS l, value % 2 AS parity FROM sql_regression_samples
GROUP BY parity, l ORDER BY parity, l;

-- The table column wins over the alias, so 'a' and 'A' stay separate groups.
SELECT lower(label) AS label, COUNT(*), SUM(value) FROM sql_regression_samples
GROUP BY label ORDER BY 1, 3;

SELECT value % 2 AS parity, COUNT(*) FROM sql_regression_samples
GROUP BY parity HAVING COUNT(*) > 3 ORDER BY parity;

SELECT lower(label), COUNT(*) FROM sql_regression_samples
GROUP BY lower(label) HAVING COUNT(*) > 1 ORDER BY 1;

SELECT COUNT(*), SUM(value) FROM sql_regression_samples
GROUP BY value % 3 HAVING SUM(value) > 4 ORDER BY 2;

SELECT lower(label) AS label, COUNT(*) FROM sql_regression_samples
GROUP BY label HAVING COUNT(*) >= 1 ORDER BY 1, 2;
