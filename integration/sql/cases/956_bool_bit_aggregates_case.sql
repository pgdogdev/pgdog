-- description: bool_and, bool_or, every, bit_and and bit_or merge across shards
-- tags: standard sharded
-- transactional: true

SELECT bool_and(flag), bool_or(flag), every(flag) FROM sql_regression_samples;

SELECT bit_and(small), bit_or(small), bit_and(num), bit_or(num), bit_and(big), bit_or(big)
FROM sql_regression_samples;

SELECT grp, bool_and(flag), bool_or(flag), every(flag), bit_and(num), bit_or(num)
FROM sql_regression_samples GROUP BY grp ORDER BY grp;

SELECT bool_and(flag), bool_or(flag), bit_and(num), bit_or(num)
FROM sql_regression_samples WHERE id < 0;

SELECT bool_and(flag), bool_or(flag), bit_and(num), bit_or(num)
FROM sql_regression_samples WHERE flag IS NULL;

SELECT bool_or(DISTINCT flag), bit_or(DISTINCT num), bool_and(num > 2) FILTER (WHERE num IS NOT NULL)
FROM sql_regression_samples;

SELECT grp FROM sql_regression_samples GROUP BY grp HAVING bool_or(flag) ORDER BY grp;
