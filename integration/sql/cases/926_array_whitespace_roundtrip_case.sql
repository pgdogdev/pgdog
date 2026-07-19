-- description: Array literals with whitespace round-trip after PostgreSQL normalization
-- tags: standard
-- transactional: true

SELECT id, sample_int_array, sample_text_array FROM sql_regression_samples ORDER BY id;
