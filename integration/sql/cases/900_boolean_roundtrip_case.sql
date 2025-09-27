-- description: Boolean values round-trip through PgDog
-- tags: standard
-- transactional: true

SELECT id, sample_boolean FROM sql_regression_samples ORDER BY id;
