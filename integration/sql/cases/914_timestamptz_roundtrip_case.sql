-- description: TIMESTAMPTZ values round-trip across time zones and precision
-- tags: standard
-- transactional: true

SELECT id, sample_timestamptz FROM sql_regression_samples ORDER BY id;
