DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_int_array INTEGER[],
    sample_text_array TEXT[]
);
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (1, '{1,2,3}', '{"hello","world"}');
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (2, '{-1,0,2147483647}', '{"has,comma","has space"}');
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (3, '{}', '{}');
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (4, '{NULL,1,NULL}', '{NULL,"value",NULL}');
INSERT INTO sql_regression_samples (id, sample_int_array, sample_text_array) VALUES (5, NULL, NULL);
