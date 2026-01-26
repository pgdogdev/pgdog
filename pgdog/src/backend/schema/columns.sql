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
ORDER BY
    c.table_schema, c.table_name, c.ordinal_position;
