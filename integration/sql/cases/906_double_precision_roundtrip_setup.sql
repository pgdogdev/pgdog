DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_double DOUBLE PRECISION
);
INSERT INTO sql_regression_samples (id, sample_double) VALUES (1, 1.5::DOUBLE PRECISION);
INSERT INTO sql_regression_samples (id, sample_double) VALUES (2, -1234567890.9876543::DOUBLE PRECISION);
INSERT INTO sql_regression_samples (id, sample_double) VALUES (3, 'Infinity'::DOUBLE PRECISION);
INSERT INTO sql_regression_samples (id, sample_double) VALUES (4, '-Infinity'::DOUBLE PRECISION);
INSERT INTO sql_regression_samples (id, sample_double) VALUES (5, 2.2250738585072014e-308::DOUBLE PRECISION);
INSERT INTO sql_regression_samples (id, sample_double) VALUES (6, NULL);
