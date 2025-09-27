-- description: NUMERIC values round-trip across sign and scale variations
-- tags: standard
-- transactional: true

SELECT id, sample_numeric FROM sql_regression_samples ORDER BY id;
