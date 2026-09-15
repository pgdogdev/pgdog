-- Postgres-side prerequisites for the Google access-token suite.
--
-- Run directly against PostgreSQL (not through PgDog) as the `pgdog` service
-- account. `run.sh` applies this before starting PgDog.

-- Role impersonated after a Google login as alice@example.com. It needs no
-- LOGIN: PgDog connects as the `pgdog` service account and assumes the role
-- through the `role` startup parameter.
DROP ROLE IF EXISTS "alice@example.com";
CREATE ROLE "alice@example.com" NOLOGIN;
GRANT "alice@example.com" TO pgdog;

-- dave@example.com deliberately has NO role here: his pool is configured, so
-- his Google login succeeds, but the backend connection must fail instead of
-- silently running queries as the service account.
DROP ROLE IF EXISTS "dave@example.com";
