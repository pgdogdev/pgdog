-- description: Cross-shard GROUP BY merges on keys that aren't in the SELECT list
-- tags: standard sharded
-- transactional: true

SELECT category FROM sql_regression_samples GROUP BY category, region ORDER BY category;
SELECT category, count(*) FROM sql_regression_samples GROUP BY category, region ORDER BY 1, 2;
SELECT count(*) FROM sql_regression_samples GROUP BY upper(region) ORDER BY 1;
SELECT 1 AS one FROM sql_regression_samples GROUP BY lower(region);
