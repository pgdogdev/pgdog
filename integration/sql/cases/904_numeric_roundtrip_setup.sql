DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_numeric NUMERIC(30,10)
);
INSERT INTO sql_regression_samples (id, sample_numeric) VALUES (1, 123456789.987654321::NUMERIC);
INSERT INTO sql_regression_samples (id, sample_numeric) VALUES (2, -987654321.123456789::NUMERIC);
INSERT INTO sql_regression_samples (id, sample_numeric) VALUES (3, 0.0000001234::NUMERIC);
INSERT INTO sql_regression_samples (id, sample_numeric) VALUES (4, NULL);
