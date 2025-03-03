-- Schema where we are placing all of our code.
CREATE SCHEMA IF NOT EXISTS pgdog;

GRANT USAGE ON SCHEMA pgdog TO PUBLIC;

-- Table to use with "satisfies_hash_partition".
-- We just need the type to match; everything else
-- is passed as an argument to the function.
CREATE TABLE IF NOT EXISTS pgdog.validator_bigint (id BIGINT NOT NULL PRIMARY KEY)
PARTITION BY
    HASH (id);
