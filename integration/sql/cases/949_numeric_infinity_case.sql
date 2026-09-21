-- description: Numeric infinities preserve PostgreSQL ordering and aggregate results across shards
-- tags: sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_sharded_text pgdog_sharded_binary

SELECT id, value FROM sql_regression_samples ORDER BY value, id;
SELECT id, value FROM sql_regression_samples ORDER BY value DESC, id;
SELECT id, ARRAY[value] AS vals FROM sql_regression_samples ORDER BY vals, id;
SELECT value, COUNT(*) FROM sql_regression_samples GROUP BY value ORDER BY value;
SELECT kind, MIN(value), MAX(value), SUM(value), AVG(value)
FROM sql_regression_samples GROUP BY kind ORDER BY kind;
SELECT kind, VAR_POP(value), VAR_SAMP(value), STDDEV_POP(value), STDDEV_SAMP(value)
FROM sql_regression_samples WHERE kind <> 'finite' GROUP BY kind ORDER BY kind;
