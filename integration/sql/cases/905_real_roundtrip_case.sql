-- description: REAL (float4) values round-trip including infinities and NULL
-- tags: standard
-- transactional: true

SELECT id, sample_real FROM sql_regression_samples ORDER BY id;
