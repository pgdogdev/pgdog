DROP TABLE IF EXISTS sql_regression_samples;
CREATE TABLE sql_regression_samples (
    id BIGINT PRIMARY KEY,
    sample_macaddr MACADDR,
    sample_macaddr8 MACADDR8
);
INSERT INTO sql_regression_samples (id, sample_macaddr, sample_macaddr8)
    VALUES (1, '08:00:2b:01:02:03'::MACADDR, '08:00:2b:ff:fe:01:02:03'::MACADDR8);
INSERT INTO sql_regression_samples (id, sample_macaddr, sample_macaddr8)
    VALUES (2, 'AA-BB-CC-DD-EE-FF'::MACADDR, 'aa:bb:cc:dd:ee:ff:00:11'::MACADDR8);
INSERT INTO sql_regression_samples (id, sample_macaddr, sample_macaddr8)
    VALUES (3, NULL, NULL);
