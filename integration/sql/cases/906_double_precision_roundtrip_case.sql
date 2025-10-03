-- description: DOUBLE PRECISION values round-trip including denormal and infinities
-- tags: standard
-- transactional: true

SELECT id, sample_double FROM sql_regression_samples ORDER BY id;
