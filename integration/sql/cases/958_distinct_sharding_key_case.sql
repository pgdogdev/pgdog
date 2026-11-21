-- description: DISTINCT aggregates over the sharding key merge like the plain aggregates
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

SELECT sum(DISTINCT id), avg(DISTINCT id) FROM sql_regression_samples;

SELECT sum(DISTINCT id)::bigint, avg(DISTINCT id)::float8, count(DISTINCT id) FROM sql_regression_samples;

SELECT grp, sum(DISTINCT id), avg(DISTINCT id) FROM sql_regression_samples GROUP BY grp ORDER BY grp;

SELECT grp, avg(DISTINCT s.id), avg(amount) FROM sql_regression_samples s
GROUP BY grp HAVING count(*) > 1 ORDER BY grp;

SELECT sum(DISTINCT id) FILTER (WHERE amount >= 10), avg(DISTINCT id) FILTER (WHERE amount >= 10)
FROM sql_regression_samples;

SELECT sum(DISTINCT id), avg(DISTINCT id) FROM sql_regression_samples WHERE id < 0;
