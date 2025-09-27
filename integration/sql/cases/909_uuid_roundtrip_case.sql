-- description: UUID values round-trip including all-zero and all-ones patterns
-- tags: standard
-- transactional: true

SELECT id, sample_uuid FROM sql_regression_samples ORDER BY id;
