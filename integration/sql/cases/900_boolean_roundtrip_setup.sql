DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_boolean BOOLEAN
);
INSERT INTO sql_regression_samples (id, sample_boolean) VALUES (1, TRUE);
INSERT INTO sql_regression_samples (id, sample_boolean) VALUES (2, FALSE);
INSERT INTO sql_regression_samples (id, sample_boolean) VALUES (3, NULL);
