-- description: Array constructor expressions and casts round-trip through PgDog
-- tags: standard
-- transactional: true

SELECT
    ARRAY[1,2,3] AS int_array,
    ARRAY['hello','world'] AS text_array,
    ARRAY[true,false,NULL::boolean] AS bool_array,
    ARRAY[1.5,2.5]::real[] AS real_array,
    ARRAY[]::integer[] AS empty_int,
    ARRAY[NULL,1,NULL]::integer[] AS nulls_int,
    ARRAY['has,comma','has"quote','has\back'] AS special_text
FROM sql_regression_samples
WHERE id = 1;
