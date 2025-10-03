-- description: TIMESTAMP WITHOUT TIME ZONE values round-trip across precision levels
-- tags: standard
-- transactional: true

SELECT id, sample_timestamp FROM sql_regression_samples ORDER BY id;
