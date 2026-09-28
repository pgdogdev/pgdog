-- description: Timestamp fractions retain their scale during cross-shard sorting and aggregation
-- tags: standard sharded
-- transactional: true

SELECT id, sample_timestamp FROM sql_regression_samples ORDER BY sample_timestamp, id;
SELECT id, sample_timestamp FROM sql_regression_samples ORDER BY sample_timestamp DESC, id;
SELECT MIN(sample_timestamp), MAX(sample_timestamp) FROM sql_regression_samples;
SELECT sample_timestamp, COUNT(*) FROM sql_regression_samples GROUP BY sample_timestamp ORDER BY sample_timestamp;

SELECT id, sample_timestamptz FROM sql_regression_samples ORDER BY sample_timestamptz, id;
SELECT id, sample_timestamptz FROM sql_regression_samples ORDER BY sample_timestamptz DESC, id;
SELECT MIN(sample_timestamptz), MAX(sample_timestamptz) FROM sql_regression_samples;
SELECT sample_timestamptz, COUNT(*) FROM sql_regression_samples GROUP BY sample_timestamptz ORDER BY sample_timestamptz;
