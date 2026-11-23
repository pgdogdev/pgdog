-- description: Cross-shard ORDER BY honors NULLS FIRST / NULLS LAST
-- tags: standard sharded
-- transactional: true

SELECT id, score FROM sql_regression_samples ORDER BY score, id;
SELECT id, score FROM sql_regression_samples ORDER BY score DESC, id;
SELECT id, score FROM sql_regression_samples ORDER BY score ASC NULLS FIRST, id;
SELECT id, score FROM sql_regression_samples ORDER BY score ASC NULLS LAST, id;
SELECT id, score FROM sql_regression_samples ORDER BY score DESC NULLS FIRST, id;
SELECT id, score FROM sql_regression_samples ORDER BY score DESC NULLS LAST, id;
SELECT id, label FROM sql_regression_samples ORDER BY 2 NULLS FIRST, 1;
SELECT id, label FROM sql_regression_samples ORDER BY 2 DESC NULLS LAST, 1;
SELECT id FROM sql_regression_samples ORDER BY score NULLS FIRST, id;
SELECT id FROM sql_regression_samples ORDER BY label DESC NULLS LAST, id;
