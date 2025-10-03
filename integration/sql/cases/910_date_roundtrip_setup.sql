DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_date DATE
);
INSERT INTO sql_regression_samples (id, sample_date) VALUES (1, DATE '0001-01-01');
INSERT INTO sql_regression_samples (id, sample_date) VALUES (2, DATE '1970-01-01');
INSERT INTO sql_regression_samples (id, sample_date) VALUES (3, DATE '2020-02-29');
INSERT INTO sql_regression_samples (id, sample_date) VALUES (4, NULL);
