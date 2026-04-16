-- description: TEXT[] arrays round-trip including quoting edge cases
-- tags: standard
-- transactional: true

SELECT id, sample_text_array FROM sql_regression_samples ORDER BY id;
