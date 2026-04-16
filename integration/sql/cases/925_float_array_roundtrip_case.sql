-- description: REAL[] and DOUBLE PRECISION[] arrays round-trip including NaN/Infinity
-- tags: standard
-- transactional: true

SELECT id, sample_real_array, sample_dp_array FROM sql_regression_samples ORDER BY id;
