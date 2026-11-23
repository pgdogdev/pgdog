DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    category TEXT,
    region TEXT
);

INSERT INTO sql_regression_samples (id, category, region) VALUES (1, 'drinks', 'eu');
INSERT INTO sql_regression_samples (id, category, region) VALUES (101, 'drinks', 'eu');
INSERT INTO sql_regression_samples (id, category, region) VALUES (2, 'drinks', 'us');
INSERT INTO sql_regression_samples (id, category, region) VALUES (102, 'food', 'eu');
INSERT INTO sql_regression_samples (id, category, region) VALUES (3, 'food', 'eu');
INSERT INTO sql_regression_samples (id, category, region) VALUES (103, NULL, 'us');
INSERT INTO sql_regression_samples (id, category, region) VALUES (4, NULL, 'us');
