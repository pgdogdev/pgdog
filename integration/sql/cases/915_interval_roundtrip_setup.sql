DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_interval INTERVAL
);
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (1, INTERVAL '1 year 2 months 3 days 04:05:06');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (2, INTERVAL '-3 days');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (3, INTERVAL '15 minutes 30 seconds');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (4, INTERVAL '0');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (5, NULL);
