-- description: Money compares by amount during cross-shard sorting and aggregation in text format
-- tags: standard sharded
-- transactional: true
-- skip-targets: postgres_standard_binary pgdog_standard_binary pgdog_sharded_binary

SELECT id, sample_money FROM sql_regression_samples ORDER BY sample_money, id;
SELECT id, sample_money FROM sql_regression_samples ORDER BY sample_money DESC, id;
SELECT MIN(sample_money), MAX(sample_money) FROM sql_regression_samples;
