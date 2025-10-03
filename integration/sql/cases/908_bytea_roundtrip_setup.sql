DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_bytea BYTEA
);
INSERT INTO sql_regression_samples (id, sample_bytea) VALUES (1, decode('00ff10', 'hex'));
INSERT INTO sql_regression_samples (id, sample_bytea) VALUES (2, decode('deadbeefcafebabe', 'hex'));
INSERT INTO sql_regression_samples (id, sample_bytea) VALUES (3, decode('', 'hex'));
INSERT INTO sql_regression_samples (id, sample_bytea) VALUES (4, decode('5c78', 'hex'));
INSERT INTO sql_regression_samples (id, sample_bytea) VALUES (5, NULL);
