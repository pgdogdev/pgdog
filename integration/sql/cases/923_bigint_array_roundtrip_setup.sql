DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_bigint_array BIGINT[]
);
INSERT INTO sql_regression_samples (id, sample_bigint_array) VALUES (1, '{1,2,3}');
INSERT INTO sql_regression_samples (id, sample_bigint_array) VALUES (2, '{-9223372036854775808,0,9223372036854775807}');
INSERT INTO sql_regression_samples (id, sample_bigint_array) VALUES (3, '{}');
INSERT INTO sql_regression_samples (id, sample_bigint_array) VALUES (4, NULL);
