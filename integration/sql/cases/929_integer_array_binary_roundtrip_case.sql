-- description: Integer and text arrays round-trip through PgDog in binary format
-- tags: standard
-- transactional: true
-- only-targets: postgres_standard_binary pgdog_standard_binary pgdog_sharded_binary

SELECT id, sample_int_array, sample_text_array FROM sql_regression_samples ORDER BY id;
