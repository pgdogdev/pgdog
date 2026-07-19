DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_bool_array BOOLEAN[]
);
INSERT INTO sql_regression_samples (id, sample_bool_array) VALUES (1, '{t,f,t}');
INSERT INTO sql_regression_samples (id, sample_bool_array) VALUES (2, '{NULL,t,NULL}');
INSERT INTO sql_regression_samples (id, sample_bool_array) VALUES (3, '{}');
INSERT INTO sql_regression_samples (id, sample_bool_array) VALUES (4, NULL);
