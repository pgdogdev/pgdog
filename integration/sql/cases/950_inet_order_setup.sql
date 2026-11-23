DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_inet INET,
    sample_cidr CIDR
);

INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (1, '9.0.0.1', '9.0.0.0/8');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (102, '10.0.0.10', '10.0.0.0/8');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (3, '10.0.0.0/8', '10.0.0.0/16');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (104, '::1', '::1/128');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (5, '192.168.1.5/24', '192.168.1.0/24');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (106, 'fe80::1', 'fe80::/64');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (107, '9.0.0.1', '9.0.0.0/8');
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr) VALUES (8, NULL, NULL);
