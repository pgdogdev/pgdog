DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_bigint BIGINT
);
INSERT INTO sql_regression_samples (id, sample_bigint) VALUES (1, (-9223372036854775808)::BIGINT);
INSERT INTO sql_regression_samples (id, sample_bigint) VALUES (2, 0::BIGINT);
INSERT INTO sql_regression_samples (id, sample_bigint) VALUES (3, 9223372036854775807::BIGINT);
INSERT INTO sql_regression_samples (id, sample_bigint) VALUES (4, NULL);
