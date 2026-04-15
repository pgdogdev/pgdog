DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_int_array INTEGER[],
    sample_text_array TEXT[]
);
-- Whitespace around elements (PostgreSQL normalizes on input)
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (1, '{ 1 , 2 , 3 }', '{ hello , world }');
-- Leading/trailing whitespace on the literal
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (2, ' {4,5,6} ', ' {foo,bar} ');
-- Whitespace around NULL
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (3, '{ NULL , 1 }', '{ NULL , a }');
-- Whitespace with empty array
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (4, ' {} ', ' {} ');
-- Whitespace with quoted elements (spaces inside quotes are preserved)
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (5, '{1,2}', '{ "has space" , plain }');
