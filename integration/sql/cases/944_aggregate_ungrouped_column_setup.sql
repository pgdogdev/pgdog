DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    org_id BIGINT,
    extra TEXT,
    val BIGINT
);

INSERT INTO sql_regression_samples (id, org_id, extra, val) VALUES (1, 10, 'alpha', 100);
INSERT INTO sql_regression_samples (id, org_id, extra, val) VALUES (2, 10, 'beta', 200);
INSERT INTO sql_regression_samples (id, org_id, extra, val) VALUES (3, 20, 'alpha', 300);
INSERT INTO sql_regression_samples (id, org_id, extra, val) VALUES (4, 10, 'alpha', 400);
INSERT INTO sql_regression_samples (id, org_id, extra, val) VALUES (5, 20, 'beta', 500);
INSERT INTO sql_regression_samples (id, org_id, extra, val) VALUES (6, 20, 'alpha', 600);
