DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_interval_array INTERVAL[]
);

INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (1, ARRAY[INTERVAL '1 year 2 months 1 day 04:05:06.7']);
INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (2, ARRAY[INTERVAL '1 year 2 months 1 day 04:05:06.7']);
INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (3, ARRAY[INTERVAL '00:00:00.006']);
INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (4, ARRAY[INTERVAL '00:00:00.006']);
INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (5, ARRAY[NULL::INTERVAL, INTERVAL '00:15:30.25']);
INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (6, ARRAY[NULL::INTERVAL, INTERVAL '00:15:30.25']);
INSERT INTO sql_regression_samples (id, sample_interval_array) VALUES (7, ARRAY[]::INTERVAL[]);
