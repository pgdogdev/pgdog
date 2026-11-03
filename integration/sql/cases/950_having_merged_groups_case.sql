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
GROUP BY grp HAVING NOT (COUNT(*) BETWEEN 2 AND 3) ORDER BY grp;

SELECT COUNT(*) FROM sql_regression_samples HAVING COUNT(*) > 5;

SELECT COUNT(*) FROM sql_regression_samples HAVING COUNT(*) > 100;
