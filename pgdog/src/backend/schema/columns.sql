SELECT
    c.table_catalog::text,
    c.table_schema::text,
    c.table_name::text,
    c.column_name::text,
    c.column_default::text,
    (c.is_nullable != 'NO')::text AS is_nullable,
    c.data_type::text,
    c.ordinal_position::int,
    (pk.column_name IS NOT NULL)::text AS is_primary_key
FROM
    information_schema.columns c
LEFT JOIN (
    SELECT
        kcu.table_schema,
        kcu.table_name,
        kcu.column_name
    FROM
        information_schema.table_constraints tc
    JOIN
        information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE
        tc.constraint_type = 'PRIMARY KEY'
) pk ON c.table_schema = pk.table_schema
    AND c.table_name = pk.table_name
    AND c.column_name = pk.column_name
WHERE
    c.table_schema NOT IN ('pg_catalog', 'information_schema')

UNION ALL

-- Materialized views are not in information_schema.columns
SELECT
    current_database()::text AS table_catalog,
    n.nspname::text AS table_schema,
    cls.relname::text AS table_name,
    a.attname::text AS column_name,
    pg_get_expr(d.adbin, d.adrelid)::text AS column_default,
    (NOT a.attnotnull)::text AS is_nullable,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    a.attnum::int AS ordinal_position,
    'false'::text AS is_primary_key
FROM
    pg_catalog.pg_class cls
JOIN
    pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
JOIN
    pg_catalog.pg_attribute a ON a.attrelid = cls.oid
LEFT JOIN
    pg_catalog.pg_attrdef d ON d.adrelid = cls.oid AND d.adnum = a.attnum
WHERE
    cls.relkind = 'm'
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')

ORDER BY
    table_schema, table_name, ordinal_position;
