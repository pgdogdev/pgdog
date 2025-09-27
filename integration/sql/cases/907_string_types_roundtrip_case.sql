-- description: TEXT, VARCHAR, and CHAR string types round-trip across PgDog
-- tags: standard
-- transactional: true

SELECT id, sample_text, sample_varchar, sample_char FROM sql_regression_samples ORDER BY id;
