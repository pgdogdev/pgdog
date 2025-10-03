-- description: BYTEA values round-trip including binary, zero-length, and NULL payloads
-- tags: standard
-- transactional: true

SELECT id, sample_bytea FROM sql_regression_samples ORDER BY id;
