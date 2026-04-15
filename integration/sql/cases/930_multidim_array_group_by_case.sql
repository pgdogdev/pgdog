-- description: Multidimensional arrays survive GROUP BY round-trips through PgDog
-- tags: standard sharded
-- transactional: true

SELECT
    MIN(id) AS first_id,
    sample_matrix,
    COUNT(*) AS occurrences
FROM sql_regression_samples
GROUP BY sample_matrix
ORDER BY first_id;
