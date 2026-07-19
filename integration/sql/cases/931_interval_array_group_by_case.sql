-- description: Interval arrays survive binary and sharded GROUP BY round-trips through PgDog
-- tags: standard sharded
-- transactional: true

SELECT
    MIN(id) AS first_id,
    sample_interval_array,
    COUNT(*) AS occurrences
FROM sql_regression_samples
GROUP BY sample_interval_array
ORDER BY first_id;
