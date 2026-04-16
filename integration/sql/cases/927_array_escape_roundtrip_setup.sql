DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_text_array TEXT[]
);
-- Unquoted backslash escape: \, means literal comma (one element)
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (1, E'{a\\,b}');
-- Unquoted backslash escape: \\ means literal backslash
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (2, E'{a\\\\b}');
-- Unquoted backslash escape: \} means literal brace
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (3, E'{a\\}b}');
-- Mixed quoted and unquoted escapes
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (4, E'{"quoted\\\\bslash",unquoted}');
-- Multiple escape sequences
INSERT INTO sql_regression_samples (id, sample_text_array) VALUES (5, E'{a\\,b\\,c}');
