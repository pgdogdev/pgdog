-- description: sum and avg over DISTINCT sharding key merge like the plain aggregates
-- tags: standard sharded
-- transactional: true

SELECT sum(DISTINCT id), avg(DISTINCT id) FROM sql_regression_samples;

SELECT sum(DISTINCT id)::bigint, avg(DISTINCT id)::float8, count(DISTINCT id) FROM sql_regression_samples;

SELECT grp, sum(DISTINCT id), avg(DISTINCT id) FROM sql_regression_samples GROUP BY grp ORDER BY grp;

SELECT grp, avg(DISTINCT s.id), avg(amount) FROM sql_regression_samples s
GROUP BY grp HAVING count(*) > 1 ORDER BY grp;

SELECT sum(DISTINCT id) FILTER (WHERE amount >= 10), avg(DISTINCT id) FILTER (WHERE amount >= 10)
FROM sql_regression_samples;

SELECT sum(DISTINCT id), avg(DISTINCT id) FROM sql_regression_samples WHERE id < 0;
