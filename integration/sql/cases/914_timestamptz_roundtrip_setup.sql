DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_timestamptz TIMESTAMPTZ
);
INSERT INTO sql_regression_samples (id, sample_timestamptz) VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00');
INSERT INTO sql_regression_samples (id, sample_timestamptz) VALUES (2, TIMESTAMPTZ '1999-12-31 23:59:59.123456+05:30');
INSERT INTO sql_regression_samples (id, sample_timestamptz) VALUES (3, TIMESTAMPTZ '2020-02-29 12:00:00-07');
INSERT INTO sql_regression_samples (id, sample_timestamptz) VALUES (4, NULL);
