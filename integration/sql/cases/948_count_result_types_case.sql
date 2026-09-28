-- description: Casted counts preserve their result types when merged across shards
-- tags: standard sharded
-- transactional: true

SELECT COUNT(*)::smallint, COUNT(*)::integer, COUNT(*)::bigint,
       COUNT(*)::real, COUNT(*)::double precision
FROM sql_regression_samples;

SELECT COUNT(value)::smallint, COUNT(value)::integer, COUNT(value)::bigint,
       COUNT(value)::real, COUNT(value)::double precision
FROM sql_regression_samples WHERE id < 0;

SELECT COUNT(value)::smallint, COUNT(value)::integer, COUNT(value)::bigint,
       COUNT(value)::real, COUNT(value)::double precision
FROM sql_regression_samples WHERE value IS NULL;

SELECT value, COUNT(*)::smallint, COUNT(*)::integer, COUNT(*)::bigint,
       COUNT(*)::real, COUNT(*)::double precision
FROM sql_regression_samples GROUP BY value ORDER BY value;
