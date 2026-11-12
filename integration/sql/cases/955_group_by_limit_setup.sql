DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    category TEXT,
    amount BIGINT
);

-- Shard 0: x has the most rows, but y has the most overall.
INSERT INTO sql_regression_samples (id, category, amount) VALUES (1, 'x', 10);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (2, 'x', 10);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (3, 'x', 10);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (4, 'x', 10);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (5, 'x', 10);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (6, 'z', 20);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (7, 'z', 20);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (8, 'z', 20);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (9, 'z', 20);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (10, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (11, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (12, 'y', 30);

-- Shard 1
INSERT INTO sql_regression_samples (id, category, amount) VALUES (101, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (102, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (103, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (104, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (105, 'y', 30);
INSERT INTO sql_regression_samples (id, category, amount) VALUES (106, 'x', 10);
