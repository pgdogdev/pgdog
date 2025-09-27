DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_jsonb JSONB
);
INSERT INTO sql_regression_samples (id, sample_jsonb) VALUES (1, '{"simple":"value"}'::JSONB);
INSERT INTO sql_regression_samples (id, sample_jsonb) VALUES (2, '{"nested":{"a":1,"b":[true,false]},"c":null}'::JSONB);
INSERT INTO sql_regression_samples (id, sample_jsonb) VALUES (3, '[1,2,3,4]'::JSONB);
INSERT INTO sql_regression_samples (id, sample_jsonb) VALUES (4, '"scalar"'::JSONB);
INSERT INTO sql_regression_samples (id, sample_jsonb) VALUES (5, NULL);
