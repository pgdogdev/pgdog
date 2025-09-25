DROP TABLE IF EXISTS sql_regression_sample;
CREATE TABLE sql_regression_sample (
    id BIGINT PRIMARY KEY,
    val TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO sql_regression_sample (id, val)
VALUES
    (1, 'alpha'),
    (2, 'beta'),
    (3, 'gamma')
ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
