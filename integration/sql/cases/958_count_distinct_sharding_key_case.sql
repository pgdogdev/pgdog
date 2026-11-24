-- description: count(DISTINCT sharding key) adds up each shard's count
-- tags: standard sharded
-- transactional: true

SELECT count(DISTINCT id) FROM sql_regression_samples;

SELECT count(DISTINCT id)::int, count(*) FROM sql_regression_samples;

SELECT count(DISTINCT sql_regression_samples.id) FROM sql_regression_samples;

SELECT count(DISTINCT s.id) FROM sql_regression_samples s WHERE amount > 5;

SELECT grp, count(DISTINCT id) FROM sql_regression_samples GROUP BY grp ORDER BY grp;

SELECT grp, count(DISTINCT id), sum(amount) FROM sql_regression_samples
GROUP BY grp HAVING count(*) > 1 ORDER BY grp;

SELECT count(DISTINCT id) FILTER (WHERE amount >= 10) FROM sql_regression_samples;

SELECT count(DISTINCT id) FROM sql_regression_samples WHERE id < 0;
