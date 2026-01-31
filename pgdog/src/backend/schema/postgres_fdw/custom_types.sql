-- Query to fetch custom type definitions (enums, domains, composite types)
-- for recreation on FDW server before creating foreign tables.

-- Enums: type info and values
SELECT
    'enum' AS type_kind,
    n.nspname::text AS schema_name,
    t.typname::text AS type_name,
    NULL::text AS base_type,
    NULL::text AS constraint_def,
    NULL::text AS default_value,
    NULL::text AS collation_name,
    NULL::text AS collation_schema,
    string_agg(e.enumlabel::text, ',' ORDER BY e.enumsortorder)::text AS enum_labels,
    NULL::text AS composite_attributes
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
WHERE t.typtype = 'e'
    AND n.nspname <> 'pg_catalog'
    AND n.nspname !~ '^pg_toast'
    AND n.nspname <> 'information_schema'
GROUP BY n.nspname, t.typname

UNION ALL

-- Domains: base type, constraints, defaults, collation
SELECT
    'domain' AS type_kind,
    n.nspname::text AS schema_name,
    t.typname::text AS type_name,
    pg_catalog.format_type(t.typbasetype, t.typtypmod)::text AS base_type,
    (
        SELECT string_agg(pg_catalog.pg_get_constraintdef(c.oid, true), ' ' ORDER BY c.conname)
        FROM pg_catalog.pg_constraint c
        WHERE c.contypid = t.oid
    )::text AS constraint_def,
    t.typdefault::text AS default_value,
    coll.collname::text AS collation_name,
    collnsp.nspname::text AS collation_schema,
    NULL::text AS enum_labels,
    NULL::text AS composite_attributes
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
LEFT JOIN pg_catalog.pg_collation coll ON coll.oid = t.typcollation
LEFT JOIN pg_catalog.pg_namespace collnsp ON collnsp.oid = coll.collnamespace
WHERE t.typtype = 'd'
    AND n.nspname <> 'pg_catalog'
    AND n.nspname !~ '^pg_toast'
    AND n.nspname <> 'information_schema'

UNION ALL

-- Composite types (excluding table row types)
-- Uses newline as separator since type definitions can contain commas
SELECT
    'composite' AS type_kind,
    n.nspname::text AS schema_name,
    t.typname::text AS type_name,
    NULL::text AS base_type,
    NULL::text AS constraint_def,
    NULL::text AS default_value,
    NULL::text AS collation_name,
    NULL::text AS collation_schema,
    NULL::text AS enum_labels,
    (
        SELECT string_agg(
            a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod),
            E'\n' ORDER BY a.attnum
        )
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = t.typrelid
            AND a.attnum > 0
            AND NOT a.attisdropped
    )::text AS composite_attributes
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
WHERE t.typtype = 'c'
    AND c.relkind = 'c'  -- Only standalone composite types, not table row types
    AND n.nspname <> 'pg_catalog'
    AND n.nspname !~ '^pg_toast'
    AND n.nspname <> 'information_schema'

ORDER BY type_kind, schema_name, type_name
