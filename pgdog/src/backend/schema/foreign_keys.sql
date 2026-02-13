SELECT
    kcu.table_schema::text AS source_schema,
    kcu.table_name::text AS source_table,
    kcu.column_name::text AS source_column,
    ccu.table_schema::text AS ref_schema,
    ccu.table_name::text AS ref_table,
    ccu.column_name::text AS ref_column,
    rc.delete_rule::text AS on_delete,
    rc.update_rule::text AS on_update
FROM
    information_schema.table_constraints tc
JOIN
    information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN
    information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
    AND tc.table_schema = ccu.table_schema
JOIN
    information_schema.referential_constraints rc
    ON tc.constraint_name = rc.constraint_name
    AND tc.table_schema = rc.constraint_schema
WHERE
    tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY
    kcu.table_schema, kcu.table_name, kcu.column_name;
