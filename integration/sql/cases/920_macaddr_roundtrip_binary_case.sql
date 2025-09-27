-- description: MACADDR and MACADDR8 values round-trip in binary format
-- tags: standard
-- transactional: true
-- only-targets: postgres_standard_binary pgdog_standard_binary pgdog_sharded_binary

SELECT id, sample_macaddr, sample_macaddr8 FROM sql_regression_samples ORDER BY id;
