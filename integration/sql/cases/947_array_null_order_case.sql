-- description: Array NULL elements sort after non-NULL elements across shards
-- tags: standard sharded
-- transactional: true

SELECT id, value FROM sql_regression_samples ORDER BY value, id;
SELECT id, value FROM sql_regression_samples ORDER BY value DESC, id;
SELECT value, COUNT(*) FROM sql_regression_samples GROUP BY value ORDER BY value;
SELECT MIN(value), MAX(value) FROM sql_regression_samples;
