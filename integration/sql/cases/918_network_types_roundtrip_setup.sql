DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_inet INET,
    sample_cidr CIDR
);
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr)
    VALUES (1, '127.0.0.1'::INET, '10.0.0.0/8'::CIDR);
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr)
    VALUES (2, '2001:db8::1'::INET, '2001:db8::/48'::CIDR);
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr)
    VALUES (3, '192.168.1.5/24'::INET, '192.168.1.0/24'::CIDR);
INSERT INTO sql_regression_samples (id, sample_inet, sample_cidr)
    VALUES (4, NULL, NULL);
