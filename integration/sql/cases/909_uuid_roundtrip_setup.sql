DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_uuid UUID
);
INSERT INTO sql_regression_samples (id, sample_uuid) VALUES (1, '00000000-0000-0000-0000-000000000000'::UUID);
INSERT INTO sql_regression_samples (id, sample_uuid) VALUES (2, '123e4567-e89b-12d3-a456-426614174000'::UUID);
INSERT INTO sql_regression_samples (id, sample_uuid) VALUES (3, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID);
INSERT INTO sql_regression_samples (id, sample_uuid) VALUES (4, NULL);
