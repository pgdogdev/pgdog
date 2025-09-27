DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_time TIME WITHOUT TIME ZONE
);
INSERT INTO sql_regression_samples (id, sample_time) VALUES (1, TIME '00:00:00');
INSERT INTO sql_regression_samples (id, sample_time) VALUES (2, TIME '12:34:56.789');
INSERT INTO sql_regression_samples (id, sample_time) VALUES (3, TIME '23:59:59.999999');
INSERT INTO sql_regression_samples (id, sample_time) VALUES (4, NULL);
