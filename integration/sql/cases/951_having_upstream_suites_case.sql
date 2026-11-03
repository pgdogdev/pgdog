-- description: Cross-shard HAVING queries from the Postgres, Postgres-XC, DuckDB and ORM test suites
-- tags: standard sharded
-- transactional: true

-- Postgres select_having.sql
SELECT b, c FROM sql_regression_samples
GROUP BY b, c HAVING count(*) = 1 ORDER BY b, c;

SELECT c, max(a) FROM sql_regression_samples
GROUP BY c HAVING count(*) > 2 OR min(a) = max(a) ORDER BY 2;

SELECT min(a), max(a) FROM sql_regression_samples HAVING min(a) = max(a);

SELECT min(a), max(a) FROM sql_regression_samples HAVING min(a) < max(a);

-- Postgres-XC xc_having.sql
SELECT count(*), sum(a), avg(a), b FROM sql_regression_samples
GROUP BY b HAVING b + 1 > 3 ORDER BY b;

SELECT count(*), sum(a), avg(a), b FROM sql_regression_samples
GROUP BY b HAVING avg(a) > 3.75 OR b > 2 ORDER BY b;

SELECT b FROM sql_regression_samples GROUP BY b HAVING sum(a) > 8 ORDER BY b;

-- Postgres partition_aggregate.sql
SELECT b, sum(a), avg(a), count(*), min(a), max(d) FROM sql_regression_samples
GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1;

-- DuckDB test_having.test
SELECT b, SUM(a) FROM sql_regression_samples GROUP BY b HAVING SUM(a) + 10 > 28 ORDER BY b;

-- Comparisons that depend on PostgreSQL's collation and type semantics
SELECT b, count(*) FROM sql_regression_samples GROUP BY b, name HAVING name > 'b' ORDER BY b;

SELECT b, count(*) FROM sql_regression_samples
GROUP BY b, name HAVING name COLLATE "C" < 'b' ORDER BY b;

SELECT b, max(day) FROM sql_regression_samples
GROUP BY b HAVING max(day) > DATE '2024-06-01' ORDER BY b;

SELECT b, max(gap) FROM sql_regression_samples
GROUP BY b HAVING max(gap) > INTERVAL '2 days' ORDER BY b;

SELECT b FROM sql_regression_samples GROUP BY b HAVING stddev(ratio) > 3 ORDER BY b;

SELECT flag, count(*) FROM sql_regression_samples GROUP BY flag HAVING flag;

-- ORM-generated SQL
SELECT "sql_regression_samples"."b", COUNT("sql_regression_samples"."id") AS "n"
FROM "sql_regression_samples" GROUP BY "sql_regression_samples"."b"
HAVING COUNT("sql_regression_samples"."id") BETWEEN 2 AND 3
ORDER BY "sql_regression_samples"."b" ASC;
