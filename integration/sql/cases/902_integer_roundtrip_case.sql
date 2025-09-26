-- description: INTEGER values round-trip, including signed extremes
-- tags: standard
-- transactional: true

SELECT id, sample_integer FROM sql_regression_samples ORDER BY id;
