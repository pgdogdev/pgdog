DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_json JSON
);
INSERT INTO sql_regression_samples (id, sample_json) VALUES (1, '{"simple":"value"}'::JSON);
INSERT INTO sql_regression_samples (id, sample_json) VALUES (2, '{"nested":{"a":1,"b":[true,false]},"c":null}'::JSON);
INSERT INTO sql_regression_samples (id, sample_json) VALUES (3, '[1,2,3,4]'::JSON);
INSERT INTO sql_regression_samples (id, sample_json) VALUES (4, '"scalar"'::JSON);
INSERT INTO sql_regression_samples (id, sample_json) VALUES (5, NULL);
