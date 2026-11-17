DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    grp INTEGER,
    flag BOOLEAN,
    small SMALLINT,
    num INTEGER,
    big BIGINT
);
INSERT INTO sql_regression_samples VALUES (1, 1, true, 6, 6, 6);
INSERT INTO sql_regression_samples VALUES (101, 1, true, 3, 3, 3);
INSERT INTO sql_regression_samples VALUES (2, 2, true, 12, 12, 12);
INSERT INTO sql_regression_samples VALUES (102, 2, false, 10, 10, 10);
INSERT INTO sql_regression_samples VALUES (3, 3, false, 1, 1, 1);
INSERT INTO sql_regression_samples VALUES (103, 3, NULL, NULL, NULL, NULL);
INSERT INTO sql_regression_samples VALUES (4, 4, NULL, NULL, NULL, NULL);
INSERT INTO sql_regression_samples VALUES (104, 4, NULL, NULL, NULL, NULL);
