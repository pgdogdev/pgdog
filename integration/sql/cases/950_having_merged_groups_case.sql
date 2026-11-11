-- description: Cross-shard HAVING filters the merged groups, not each shard's partial groups
-- tags: standard sharded
-- transactional: true

SELECT grp, COUNT(*) FROM sql_regression_samples
GROUP BY grp HAVING COUNT(*) > 2 ORDER BY grp;

SELECT grp, COUNT(*), MAX(amount) FROM sql_regression_samples
GROUP BY grp HAVING COUNT(*) > 1 AND MAX(amount) > 20 ORDER BY grp;

SELECT grp, SUM(amount) FROM sql_regression_samples
GROUP BY grp HAVING SUM(amount) / COUNT(*) > 15 OR grp IS NULL ORDER BY grp;

SELECT grp FROM sql_regression_samples
GROUP BY grp HAVING AVG(price) > 2 ORDER BY grp;

SELECT grp, MAX(amount) FROM sql_regression_samples
GROUP BY grp HAVING MAX(amount) > '20' ORDER BY grp;

SELECT grp, COUNT(*) FROM sql_regression_samples
GROUP BY grp HAVING COUNT(*) > 1 ORDER BY grp LIMIT 1 OFFSET 1;

SELECT grp FROM sql_regression_samples
GROUP BY grp HAVING COUNT(*) > 1 ORDER BY grp OFFSET 2;

SELECT DISTINCT COUNT(*) FROM sql_regression_samples
GROUP BY grp HAVING grp IS DISTINCT FROM 'b' ORDER BY 1;

SELECT DISTINCT COUNT(*) FROM sql_regression_samples
GROUP BY grp HAVING COUNT(*) > 1 ORDER BY 1 LIMIT 1 OFFSET 1;

SELECT COUNT(*) FROM sql_regression_samples HAVING COUNT(*) > 5;

SELECT COUNT(*) FROM sql_regression_samples HAVING COUNT(*) > 100;
