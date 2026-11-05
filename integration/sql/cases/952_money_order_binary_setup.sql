DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_money MONEY
);

INSERT INTO sql_regression_samples (id, sample_money) VALUES (1, 5.00);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (102, 1000.00);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (3, -20.50);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (104, 99.99);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (5, -3.25);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (106, 12345.67);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (107, 5.00);
INSERT INTO sql_regression_samples (id, sample_money) VALUES (8, NULL);
