-- description: SMALLINT values round-trip, including boundaries and NULL
-- tags: standard
-- transactional: true

SELECT id, sample_smallint FROM sql_regression_samples ORDER BY id;
