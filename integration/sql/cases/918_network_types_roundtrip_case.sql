-- description: INET and CIDR address types round-trip across IPv4, IPv6, and masked forms
-- tags: standard
-- transactional: true

SELECT id, sample_inet, sample_cidr FROM sql_regression_samples ORDER BY id;
