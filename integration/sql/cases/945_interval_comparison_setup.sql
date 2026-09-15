DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_interval INTERVAL
);

INSERT INTO sql_regression_samples (id, sample_interval) VALUES (1, INTERVAL '1 day');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (2, INTERVAL '24 hours');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (3, INTERVAL '1 mon');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (4, INTERVAL '30 days');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (5, INTERVAL '31 days');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (6, INTERVAL '1 mon -30 days');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (7, INTERVAL '0');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (8, INTERVAL '-1 mon');
INSERT INTO sql_regression_samples (id, sample_interval) VALUES (9, INTERVAL '-31 days');
