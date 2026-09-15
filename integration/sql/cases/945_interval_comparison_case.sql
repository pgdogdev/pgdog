-- description: Interval ordering and grouping compare combined durations across shards
-- tags: standard sharded
-- transactional: true

SELECT id, sample_interval FROM sql_regression_samples ORDER BY sample_interval, id;

SELECT id, sample_interval FROM sql_regression_samples ORDER BY sample_interval DESC, id;

SELECT sample_interval, COUNT(*) AS occurrences
FROM sql_regression_samples
GROUP BY sample_interval
ORDER BY sample_interval;

SELECT MIN(sample_interval), MAX(sample_interval) FROM sql_regression_samples;
