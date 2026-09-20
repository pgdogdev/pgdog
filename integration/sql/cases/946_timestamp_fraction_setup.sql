DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_timestamp TIMESTAMP,
    sample_timestamptz TIMESTAMPTZ
);

INSERT INTO sql_regression_samples VALUES (1, '2025-01-01 00:00:00.1', '2025-01-01 00:00:00.1+00');
INSERT INTO sql_regression_samples VALUES (102, '2025-01-01 00:00:00.01', '2025-01-01 00:00:00.01+00');
INSERT INTO sql_regression_samples VALUES (3, '2025-01-01 00:00:00.001', '2025-01-01 00:00:00.001+00');
INSERT INTO sql_regression_samples VALUES (104, '2025-01-01 00:00:00.0001', '2025-01-01 00:00:00.0001+00');
INSERT INTO sql_regression_samples VALUES (5, '2025-01-01 00:00:00.00001', '2025-01-01 00:00:00.00001+00');
INSERT INTO sql_regression_samples VALUES (106, '2025-01-01 00:00:00.000001', '2025-01-01 00:00:00.000001+00');
INSERT INTO sql_regression_samples VALUES (107, '2025-01-01 00:00:00.1', '2025-01-01 00:00:00.1+00');
INSERT INTO sql_regression_samples VALUES (8, NULL, NULL);
