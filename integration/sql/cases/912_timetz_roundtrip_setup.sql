DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_timetz TIME WITH TIME ZONE
);
INSERT INTO sql_regression_samples (id, sample_timetz) VALUES (1, TIME WITH TIME ZONE '00:00:00+00');
INSERT INTO sql_regression_samples (id, sample_timetz) VALUES (2, TIME WITH TIME ZONE '08:15:30-07');
INSERT INTO sql_regression_samples (id, sample_timetz) VALUES (3, TIME WITH TIME ZONE '23:59:59.999999+13');
INSERT INTO sql_regression_samples (id, sample_timetz) VALUES (4, NULL);
