DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_real_array REAL[],
    sample_dp_array DOUBLE PRECISION[]
);
INSERT INTO sql_regression_samples (id, sample_real_array, sample_dp_array) VALUES (1, '{1.5,2.5,3.5}', '{1.5,2.5,3.5}');
INSERT INTO sql_regression_samples (id, sample_real_array, sample_dp_array) VALUES (2, '{NaN,Infinity,-Infinity}', '{NaN,Infinity,-Infinity}');
INSERT INTO sql_regression_samples (id, sample_real_array, sample_dp_array) VALUES (3, '{NULL,0}', '{NULL,0}');
INSERT INTO sql_regression_samples (id, sample_real_array, sample_dp_array) VALUES (4, '{}', '{}');
INSERT INTO sql_regression_samples (id, sample_real_array, sample_dp_array) VALUES (5, NULL, NULL);
