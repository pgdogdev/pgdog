-- description: JSONB values round-trip including objects, arrays, and scalars
-- tags: standard
-- transactional: true

SELECT id, sample_jsonb FROM sql_regression_samples ORDER BY id;
