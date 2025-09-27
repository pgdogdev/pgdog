DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_real REAL
);
INSERT INTO sql_regression_samples (id, sample_real) VALUES (1, 1.5::REAL);
INSERT INTO sql_regression_samples (id, sample_real) VALUES (2, -2.25::REAL);
INSERT INTO sql_regression_samples (id, sample_real) VALUES (3, 'Infinity'::REAL);
INSERT INTO sql_regression_samples (id, sample_real) VALUES (4, '-Infinity'::REAL);
INSERT INTO sql_regression_samples (id, sample_real) VALUES (5, NULL);
