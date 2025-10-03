DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_integer INTEGER
);
INSERT INTO sql_regression_samples (id, sample_integer) VALUES (1, (-2147483648)::INTEGER);
INSERT INTO sql_regression_samples (id, sample_integer) VALUES (2, 0::INTEGER);
INSERT INTO sql_regression_samples (id, sample_integer) VALUES (3, 2147483647::INTEGER);
INSERT INTO sql_regression_samples (id, sample_integer) VALUES (4, NULL);
