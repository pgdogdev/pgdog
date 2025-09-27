DROP TABLE IF EXISTS sql_regression_sample;
CREATE TABLE sql_regression_sample (
    id BIGINT PRIMARY KEY,
    val TEXT,
    created_at TIMESTAMPTZ
);
-- Insert rows individually so PgDog can route each id to a single shard.
INSERT INTO sql_regression_sample (id, val, created_at) VALUES (1, 'alpha', TIMESTAMPTZ '2024-01-01T00:00:00Z');
INSERT INTO sql_regression_sample (id, val, created_at) VALUES (2, 'beta', TIMESTAMPTZ '2024-01-01T00:00:00Z');
INSERT INTO sql_regression_sample (id, val, created_at) VALUES (3, 'gamma', TIMESTAMPTZ '2024-01-01T00:00:00Z');
