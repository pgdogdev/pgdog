-- description: Columns that are neither grouped/aggregated will still be included in a cross-shard GROUP BY query
-- tags: standard sharded
-- transactional: true

-- The `extra` column isn't grouped nor is it aggregated.
-- Postgres permits it because the primary key is grouped.
-- Thus, that makes it functionally dependent, and it must still be returned.
SELECT org_id, id, extra, max(val) AS max_val
FROM sql_regression_samples
GROUP BY org_id, id
ORDER BY id;

-- Do groups spanning shards still merge into one row with correct aggregates?
SELECT org_id, min(id) AS first_id, count(*) AS occurrences, max(val) AS max_val
FROM sql_regression_samples
GROUP BY org_id
ORDER BY org_id;

-- What about if we don't use an aggregate at all?
SELECT org_id, id, extra
FROM sql_regression_samples
GROUP BY org_id, id, extra
ORDER BY id;
