-- Postgres-side prerequisites for the authentication-plugin suite.
--
-- Run directly against PostgreSQL (not through PgDog) as the `pgdog` service
-- account. `run.sh` applies this before starting PgDog.

-- Role impersonated via `impersonate:reporting`. It needs no LOGIN: PgDog
-- connects as the `pgdog` service account and assumes the role through the
-- `role` startup parameter. Grant it to the service account so a non-superuser
-- deployment could assume it too.
DROP ROLE IF EXISTS reporting;
CREATE ROLE reporting NOLOGIN;
GRANT reporting TO pgdog;

-- Target table for the read-only pool INSERT rejection test.
CREATE TABLE IF NOT EXISTS auth_test (id BIGINT);
