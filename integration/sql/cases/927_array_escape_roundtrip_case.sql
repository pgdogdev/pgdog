-- description: Array text literals with unquoted backslash escapes round-trip correctly
-- tags: standard
-- transactional: true

SELECT id, sample_text_array FROM sql_regression_samples ORDER BY id;
