-- description: Interval arrays grouped under postgres intervalstyle preserve PostgreSQL text output
-- tags: standard sharded
-- transactional: true
-- only-targets: postgres_standard_text pgdog_standard_text pgdog_sharded_text

SET intervalstyle TO postgres;

SELECT
    MIN(id) AS first_id,
    sample_interval_array,
    COUNT(*) AS occurrences
FROM sql_regression_samples
GROUP BY sample_interval_array
ORDER BY first_id;
