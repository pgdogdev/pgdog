DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_text TEXT,
    sample_varchar VARCHAR(20),
    sample_char CHAR(5)
);
INSERT INTO sql_regression_samples (id, sample_text, sample_varchar, sample_char)
    VALUES (1, 'alpha', 'alpha', 'a');
INSERT INTO sql_regression_samples (id, sample_text, sample_varchar, sample_char)
    VALUES (2, 'beta with spaces', 'beta-with-hyphen', 'beta ');
INSERT INTO sql_regression_samples (id, sample_text, sample_varchar, sample_char)
    VALUES (3, 'line\nbreak', 'trimmed', 'xyz');
INSERT INTO sql_regression_samples (id, sample_text, sample_varchar, sample_char)
    VALUES (4, '', '', '');
INSERT INTO sql_regression_samples (id, sample_text, sample_varchar, sample_char)
    VALUES (5, NULL, NULL, NULL);
