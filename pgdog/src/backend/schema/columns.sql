SELECT
    table_catalog::text,
    table_schema::text,
    table_name::text,
    column_name::text,
    column_default::text,
    (is_nullable != 'NO')::text AS is_nullable,
    data_type::text
FROM
    information_schema.columns
WHERE
    table_schema NOT IN ('pg_catalog', 'information_schema');
