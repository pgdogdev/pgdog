DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    score BIGINT,
    label TEXT
);

INSERT INTO sql_regression_samples (id, score, label) VALUES (1, 30, 'c');
INSERT INTO sql_regression_samples (id, score, label) VALUES (101, 10, NULL);
INSERT INTO sql_regression_samples (id, score, label) VALUES (2, NULL, 'a');
INSERT INTO sql_regression_samples (id, score, label) VALUES (102, 20, 'b');
INSERT INTO sql_regression_samples (id, score, label) VALUES (3, 40, NULL);
INSERT INTO sql_regression_samples (id, score, label) VALUES (103, NULL, 'd');
