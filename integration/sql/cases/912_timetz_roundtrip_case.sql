-- description: TIME WITH TIME ZONE values round-trip across offsets
-- tags: standard
-- transactional: true

SELECT id, sample_timetz FROM sql_regression_samples ORDER BY id;
