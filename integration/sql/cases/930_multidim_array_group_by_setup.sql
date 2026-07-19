DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_matrix INTEGER[][]
);

INSERT INTO sql_regression_samples (id, sample_matrix) VALUES (1, '{{1,2},{3,4}}');
INSERT INTO sql_regression_samples (id, sample_matrix) VALUES (2, '{{1,2},{3,4}}');
INSERT INTO sql_regression_samples (id, sample_matrix) VALUES (3, '{{5,6},{7,8}}');
INSERT INTO sql_regression_samples (id, sample_matrix) VALUES (4, '{{5,6},{7,8}}');
INSERT INTO sql_regression_samples (id, sample_matrix) VALUES (5, '{{1,2},{3,5}}');
