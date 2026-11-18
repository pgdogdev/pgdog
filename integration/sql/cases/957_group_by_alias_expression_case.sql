-- description: GROUP BY on an expression or a SELECT alias named after its column merges across shards
-- tags: standard sharded
-- transactional: true

-- The table column wins over the alias, so 'a' and 'A' stay separate groups.
SELECT lower(label) AS label, COUNT(*), SUM(value) FROM sql_regression_samples
GROUP BY label ORDER BY 1, 3;

SELECT lower(label) AS label, COUNT(*) FROM sql_regression_samples
GROUP BY label HAVING COUNT(*) >= 1 ORDER BY 1, 2;

SELECT lower(label), COUNT(*) FROM sql_regression_samples
GROUP BY lower(label) HAVING COUNT(*) > 1 ORDER BY 1;

SELECT COUNT(*), SUM(value) FROM sql_regression_samples
GROUP BY value % 3 HAVING SUM(value) > 4 ORDER BY 2;
