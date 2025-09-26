-- description: BIGINT values round-trip, ensuring max/min 64-bit support
-- tags: standard
-- transactional: true

SELECT id, sample_bigint FROM sql_regression_samples ORDER BY id;
