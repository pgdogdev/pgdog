DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_text_array TEXT[]
);
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (1, '{"hello","world"}');
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (2, '{"has,comma","has\"quote"}');
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (3, '{"has\\backslash","has space"}');
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (4, '{"","NULL"}');
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (5, '{NULL,"actual value",NULL}');
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (6, '{}');
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (7, NULL);
