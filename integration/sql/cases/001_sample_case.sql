-- description: Simple smoke test ensuring identical behaviour for constants and table data
-- tags: standard
-- transactional: true

SELECT id, val, created_at FROM sql_regression_sample ORDER BY id;
