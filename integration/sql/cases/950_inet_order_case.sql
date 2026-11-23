-- description: Inet and cidr compare by value during cross-shard sorting and aggregation
-- tags: standard sharded
-- transactional: true

SELECT id, sample_inet FROM sql_regression_samples ORDER BY sample_inet, id;
SELECT id, sample_inet FROM sql_regression_samples ORDER BY sample_inet DESC, id;
SELECT MIN(sample_inet), MAX(sample_inet) FROM sql_regression_samples;

SELECT id, sample_cidr FROM sql_regression_samples ORDER BY sample_cidr, id;
SELECT id, sample_cidr FROM sql_regression_samples ORDER BY sample_cidr DESC, id;
