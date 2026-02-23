-- Query to fetch installed extensions for recreation on FDW server.
-- Excludes built-in extensions that are always available.
SELECT
    e.extname::text AS extension_name,
    n.nspname::text AS schema_name,
    e.extversion::text AS version
FROM pg_catalog.pg_extension e
JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid
WHERE e.extname NOT IN ('plpgsql')  -- Exclude always-installed extensions
ORDER BY e.extname
