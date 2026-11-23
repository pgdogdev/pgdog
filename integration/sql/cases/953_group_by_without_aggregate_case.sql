-- description: GROUP BY without aggregate functions merges equal groups across shards
-- tags: standard sharded
-- transactional: true

SELECT category FROM sql_regression_samples GROUP BY category ORDER BY category;
SELECT category, region FROM sql_regression_samples GROUP BY 1, 2 ORDER BY 1, 2;
SELECT region FROM sql_regression_samples GROUP BY region ORDER BY region DESC LIMIT 1;
