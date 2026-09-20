DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (id BIGINT PRIMARY KEY, value INTEGER);
INSERT INTO sql_regression_samples VALUES (1, 1);
INSERT INTO sql_regression_samples VALUES (101, 1);
INSERT INTO sql_regression_samples VALUES (2, 2);
INSERT INTO sql_regression_samples VALUES (102, 2);
INSERT INTO sql_regression_samples VALUES (3, NULL);
INSERT INTO sql_regression_samples VALUES (103, NULL);
