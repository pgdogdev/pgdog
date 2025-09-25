-- description: Simple smoke test ensuring identical behaviour for constants and table data
-- tags: standard
-- transactional: true

-- Simple baseline query
SELECT id, val, created_at FROM sql_regression_sample ORDER BY id;

-- Aggregation and casting coverage
SELECT COUNT(*)::bigint AS total_count FROM sql_regression_sample;

-- Parameter test using portal reuse
PREPARE sample_stmt AS SELECT val FROM sql_regression_sample WHERE id = $1;
EXECUTE sample_stmt(2);
EXECUTE sample_stmt(3);
DEALLOCATE sample_stmt;
