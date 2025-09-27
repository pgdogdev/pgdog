DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_smallint SMALLINT
);
INSERT INTO sql_regression_samples (id, sample_smallint) VALUES (1, (-32768)::SMALLINT);
INSERT INTO sql_regression_samples (id, sample_smallint) VALUES (2, 0::SMALLINT);
INSERT INTO sql_regression_samples (id, sample_smallint) VALUES (3, 32767::SMALLINT);
INSERT INTO sql_regression_samples (id, sample_smallint) VALUES (4, NULL);
