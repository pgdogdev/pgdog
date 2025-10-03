DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_timestamp TIMESTAMP WITHOUT TIME ZONE
);
INSERT INTO sql_regression_samples (id, sample_timestamp) VALUES (1, TIMESTAMP '1970-01-01 00:00:00');
INSERT INTO sql_regression_samples (id, sample_timestamp) VALUES (2, TIMESTAMP '1999-12-31 23:59:59.123456');
INSERT INTO sql_regression_samples (id, sample_timestamp) VALUES (3, TIMESTAMP '2020-02-29 12:00:00');
INSERT INTO sql_regression_samples (id, sample_timestamp) VALUES (4, NULL);
